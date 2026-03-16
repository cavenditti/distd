//! HTTP runner — naïve baseline that downloads whole files via Python HTTP server + curl.
//! No delta support: every transfer downloads the full content, making this the expected
//! slowest tool and thus the denominator for the "vs Slow" speed-up column.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Instant;

use crate::metrics::{self, ProcessMonitor, RunMetrics};
use crate::workload::Workload;

use super::{wait_child_with_timeout, ToolRunner, DEFAULT_CHILD_TIMEOUT};

const HTTP_PORT: u16 = 18733;

/// Minimal Python HTTP server (stdlib, no frills).
const SIMPLE_HTTP_SERVER_PY: &str = r#"
import http.server, sys

class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=sys.argv[2], **kw)
    def log_message(self, *a):
        pass

http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), QuietHandler).serve_forever()
"#;

pub struct HttpRunner {
    work_dir: PathBuf,
}

impl HttpRunner {
    pub fn new(work_dir: &Path) -> Self {
        Self {
            work_dir: work_dir.join("http"),
        }
    }

    fn start_server(&self, serve_dir: &Path) -> Result<Child, String> {
        std::fs::create_dir_all(&self.work_dir).map_err(|e| e.to_string())?;
        let script_path = self.work_dir.join("simple_server.py");
        std::fs::write(&script_path, SIMPLE_HTTP_SERVER_PY)
            .map_err(|e| format!("write simple_server.py: {e}"))?;

        let child = Command::new("python3")
            .arg(&script_path)
            .arg(HTTP_PORT.to_string())
            .arg(serve_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("spawn http server: {e}"))?;

        // Give the server time to bind.
        std::thread::sleep(std::time::Duration::from_millis(500));
        Ok(child)
    }
}

impl ToolRunner for HttpRunner {
    fn name(&self) -> &str {
        "http"
    }

    fn is_available(&self) -> bool {
        // python3 and curl are effectively always available on macOS / modern Linux.
        super::which("python3") && super::which("curl")
    }

    fn transfer(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        m: &mut RunMetrics,
    ) -> Result<(), String> {
        let source_files = crate::workload::walkdir_files(&workload.source_dir);
        if source_files.is_empty() {
            return Err("no files in workload source".to_string());
        }

        std::fs::create_dir_all(dest_dir).map_err(|e| e.to_string())?;

        let serve_dir = &workload.source_dir;
        let mut server = self.start_server(serve_dir)?;

        // Download each source file via curl, preserving relative paths.
        let mut total_downloaded: u64 = 0;
        let mut last_error: Option<String> = None;
        let mut peak_rss: u64 = 0;

        let start = Instant::now();

        for src_file in &source_files {
            let rel = src_file
                .strip_prefix(serve_dir)
                .unwrap_or(src_file.as_path());
            let url = format!(
                "http://127.0.0.1:{}/{}",
                HTTP_PORT,
                rel.to_string_lossy().replace(' ', "%20")
            );
            let dst_path = dest_dir.join(rel);
            if let Some(parent) = dst_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }

            let mut curl = Command::new("curl")
                .args([
                    "--silent",
                    "--fail",
                    "--max-time",
                    "120",
                    "--output",
                    &dst_path.to_string_lossy(),
                    &url,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| format!("spawn curl: {e}"))?;

            let mut monitor = ProcessMonitor::new(&[curl.id()]);

            match wait_child_with_timeout(&mut curl, &mut monitor, DEFAULT_CHILD_TIMEOUT) {
                Ok(_) => {}
                Err(e) => {
                    last_error = Some(e);
                    break;
                }
            }
            let (rss, _) = monitor.finish();
            if rss > peak_rss {
                peak_rss = rss;
            }

            if dst_path.exists() {
                total_downloaded += std::fs::metadata(&dst_path).map(|md| md.len()).unwrap_or(0);
            }
        }

        let _ = server.kill();
        let _ = server.wait();

        let elapsed = start.elapsed();

        if let Some(e) = last_error {
            return Err(format!("http download failed: {e}"));
        }

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = workload.total_bytes_v1;
        m.bytes_transferred = total_downloaded;
        m.dest_size_bytes = metrics::dir_size(dest_dir);
        m.store_overhead_bytes = 0;
        m.peak_rss_bytes = peak_rss;

        // Validate by comparing per-file hashes.
        let mut all_ok = true;
        for src_file in &source_files {
            let rel = src_file
                .strip_prefix(serve_dir)
                .unwrap_or(src_file.as_path());
            let dst_file = dest_dir.join(rel);
            if !dst_file.exists() || metrics::hash_file(src_file) != metrics::hash_file(&dst_file) {
                all_ok = false;
                let dst_hash = if dst_file.exists() {
                    metrics::hash_file(&dst_file)
                } else {
                    "FILE_NOT_FOUND".into()
                };
                m.notes.push_str(&format!(
                    " | http mismatch: {} src={} dst={}",
                    rel.display(),
                    metrics::hash_file(src_file),
                    dst_hash,
                ));
            }
        }
        m.correct = all_ok;

        m.finalize();
        Ok(())
    }
}
