//! zsync runner — uses `zsyncmake` to create a .zsync control file, then `zsync` to fetch.
//! Serves files via a Python HTTP server with Range request support (required by zsync).

use std::path::{Path, PathBuf};
use std::process::{Child, Command};

use crate::metrics::{self, ProcessMonitor, RunMetrics};
use crate::workload::Workload;

use super::{ToolRunner, wait_child_with_timeout, DEFAULT_CHILD_TIMEOUT};

/// Python HTTP server script that supports Range requests (HTTP 206).
/// Python's stdlib http.server only returns 200 (full content) which zsync rejects.
const RANGE_HTTP_SERVER_PY: &str = r#"
import http.server, os, sys

class RangeHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=sys.argv[2], **kw)

    def do_GET(self):
        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            return super().do_GET()
        size = os.path.getsize(path)
        rng = self.headers.get("Range")
        if not rng or not rng.startswith("bytes="):
            return super().do_GET()
        spec = rng[6:]
        start_s, end_s = spec.split("-", 1)
        start = int(start_s) if start_s else 0
        end = int(end_s) if end_s else size - 1
        end = min(end, size - 1)
        length = end - start + 1
        self.send_response(206)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(length))
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        with open(path, "rb") as f:
            f.seek(start)
            self.wfile.write(f.read(length))

    def log_message(self, *a):
        pass  # silence

http.server.HTTPServer(("", int(sys.argv[1])), RangeHandler).serve_forever()
"#;

#[allow(dead_code)]
pub struct ZsyncRunner {
    work_dir: PathBuf,
    http_server: Option<Child>,
    http_port: u16,
}

#[allow(dead_code)]
impl ZsyncRunner {
    pub fn new(work_dir: &Path) -> Self {
        Self {
            work_dir: work_dir.join("zsync"),
            http_server: None,
            http_port: 18732,
        }
    }

    /// Start a simple HTTP server serving the source directory.
    /// Uses a custom Python script that supports Range requests (required by zsync).
    fn start_http_server(&mut self, serve_dir: &Path) -> Result<(), String> {
        self.stop_http_server();

        // Write a custom HTTP server script that supports Range requests
        let script_path = self.work_dir.join("range_server.py");
        std::fs::create_dir_all(&self.work_dir).map_err(|e| e.to_string())?;
        std::fs::write(
            &script_path,
            RANGE_HTTP_SERVER_PY,
        )
        .map_err(|e| format!("Failed to write range_server.py: {e}"))?;

        let child = Command::new("python3")
            .arg(&script_path)
            .arg(self.http_port.to_string())
            .arg(serve_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| format!("Failed to start HTTP server for zsync: {e}"))?;

        self.http_server = Some(child);
        // Give the server a moment to bind
        std::thread::sleep(std::time::Duration::from_millis(500));
        Ok(())
    }

    fn stop_http_server(&mut self) {
        if let Some(ref mut child) = self.http_server {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.http_server = None;
    }

    /// Run `zsyncmake` on a file to produce its `.zsync` control file.
    fn make_zsync_file(&self, source_file: &Path) -> Result<PathBuf, String> {
        let zsync_file = source_file.with_extension(
            format!(
                "{}.zsync",
                source_file
                    .extension()
                    .map(|e| e.to_string_lossy().to_string())
                    .unwrap_or_default()
            ),
        );

        // zsyncmake requires a URL for the file
        let filename = source_file
            .file_name()
            .ok_or("No filename")?
            .to_string_lossy();
        let url = format!("http://127.0.0.1:{}/{}", self.http_port, filename);

        let status = Command::new("zsyncmake")
            .args([
                "-u",
                &url,
                "-o",
                &zsync_file.to_string_lossy(),
                &source_file.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .status()
            .map_err(|e| format!("zsyncmake failed: {e}"))?;

        if !status.success() {
            return Err("zsyncmake exited with error".to_string());
        }
        Ok(zsync_file)
    }
}

impl ToolRunner for ZsyncRunner {
    fn name(&self) -> &str {
        "zsync"
    }

    fn is_available(&self) -> bool {
        super::which("zsync") && super::which("zsyncmake")
    }

    fn transfer(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        m: &mut RunMetrics,
    ) -> Result<(), String> {
        // zsync works on single files; for directory workloads we pack first or skip
        let source_files = crate::workload::walkdir_files(&workload.source_dir);
        if source_files.is_empty() {
            return Err("No files in workload source".to_string());
        }

        // Use the first/only file for single-file workloads; for multi-file, note limitation
        if source_files.len() > 1 {
            m.notes = format!(
                "zsync only supports single-file sync; benchmarking first file of {}",
                source_files.len()
            );
        }
        let source_file = &source_files[0];

        std::fs::create_dir_all(dest_dir).map_err(|e| e.to_string())?;

        let serve_dir = source_file.parent().unwrap_or(Path::new("."));

        // Write the Range-capable HTTP server script and start it
        let script_dir = dest_dir.join(".zsync_meta");
        std::fs::create_dir_all(&script_dir).map_err(|e| e.to_string())?;
        let script_path = script_dir.join("range_server.py");
        std::fs::write(&script_path, RANGE_HTTP_SERVER_PY)
            .map_err(|e| format!("Failed to write range_server.py: {e}"))?;

        let mut http_child = Command::new("python3")
            .arg(&script_path)
            .arg("18732")
            .arg(serve_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| format!("Failed to start HTTP server: {e}"))?;
        std::thread::sleep(std::time::Duration::from_millis(500));

        let filename = source_file.file_name().unwrap().to_string_lossy();
        let url = format!("http://127.0.0.1:18732/{filename}");

        // Create zsync file
        let zsync_dir = dest_dir.join(".zsync_meta");
        std::fs::create_dir_all(&zsync_dir).map_err(|e| e.to_string())?;
        let zsync_file = zsync_dir.join(format!("{filename}.zsync"));

        let status = Command::new("zsyncmake")
            .args([
                "-u",
                &url,
                "-o",
                &zsync_file.to_string_lossy(),
                &source_file.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| format!("zsyncmake failed: {e}"))?;

        if !status.success() {
            let _ = http_child.kill();
            return Err("zsyncmake exited with error".to_string());
        }

        // Run zsync
        let mut zsync_child = Command::new("zsync")
            .args([
                "-o",
                &dest_dir.join(&*filename).to_string_lossy(),
                &zsync_file.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("zsync failed: {e}"))?;

        let pid = zsync_child.id();
        let mut monitor = ProcessMonitor::new(&[pid]);

        let elapsed = match wait_child_with_timeout(&mut zsync_child, &mut monitor, DEFAULT_CHILD_TIMEOUT) {
            Ok(d) => d,
            Err(e) => {
                let _ = http_child.kill();
                return Err(format!("zsync: {e}"));
            }
        };
        let (peak_rss, cpu_secs) = monitor.finish();

        let _ = http_child.kill();
        let _ = http_child.wait();

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = workload.total_bytes_v1;
        m.bytes_transferred = workload.total_bytes_v1;
        m.peak_rss_bytes = peak_rss;
        m.cpu_seconds = cpu_secs;
        let meta_size = metrics::dir_size(&dest_dir.join(".zsync_meta"));
        m.dest_size_bytes = metrics::dir_size(dest_dir);
        m.store_overhead_bytes = meta_size as i64;

        // Clean up any .part file zsync leaves behind
        let part_file = dest_dir.join(format!("{filename}.part"));
        let _ = std::fs::remove_file(&part_file);

        // Validate
        let src_hash = metrics::hash_file(source_file);
        let dst_file = dest_dir.join(&*filename);
        m.correct = dst_file.exists() && metrics::hash_file(&dst_file) == src_hash;
        if !m.correct {
            let exists = dst_file.exists();
            let dst_hash = if exists {
                metrics::hash_file(&dst_file)
            } else {
                "FILE_NOT_FOUND".to_string()
            };
            m.notes
                .push_str(&format!(" | zsync correctness failed: src={src_hash} dst={dst_hash}"));
        }

        m.finalize();
        Ok(())
    }
}

impl Drop for ZsyncRunner {
    fn drop(&mut self) {
        self.stop_http_server();
    }
}
