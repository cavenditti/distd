//! rsync runner — uses `rsync` over a local daemon or direct path copy for fair comparison.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use crate::metrics::{self, ProcessMonitor, RunMetrics};
use crate::workload::Workload;

use super::{wait_child_with_timeout, ToolRunner, DEFAULT_CHILD_TIMEOUT};

#[allow(dead_code)]
pub struct RsyncRunner {
    work_dir: PathBuf,
}

impl RsyncRunner {
    pub fn new(work_dir: &Path) -> Self {
        Self {
            work_dir: work_dir.join("rsync"),
        }
    }
}

impl ToolRunner for RsyncRunner {
    fn name(&self) -> &str {
        "rsync"
    }

    fn is_available(&self) -> bool {
        super::which("rsync")
    }

    fn transfer(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        m: &mut RunMetrics,
    ) -> Result<(), String> {
        std::fs::create_dir_all(dest_dir).map_err(|e| e.to_string())?;

        let src = format!("{}/", workload.source_dir.to_string_lossy());
        let dst = format!("{}/", dest_dir.to_string_lossy());

        let start = Instant::now();
        let mut child = Command::new("rsync")
            .args([
                "-a",      // archive mode
                "--stats", // transfer statistics
                &src, &dst,
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to spawn rsync: {e}"))?;

        let pid = child.id();
        let mut monitor = ProcessMonitor::new(&[pid]);

        wait_child_with_timeout(&mut child, &mut monitor, DEFAULT_CHILD_TIMEOUT)
            .map_err(|e| format!("rsync: {e}"))?;

        let elapsed = start.elapsed();
        let output = child.wait_with_output().map_err(|e| e.to_string())?;
        let (peak_rss, cpu_secs) = monitor.finish();

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = workload.total_bytes_v1;
        m.bytes_transferred = workload.total_bytes_v1; // fallback
        m.peak_rss_bytes = peak_rss;
        m.cpu_seconds = cpu_secs;
        m.dest_size_bytes = metrics::dir_size(dest_dir);

        // Parse rsync --stats output for actual bytes sent
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(b) = parse_rsync_total_sent(&stdout) {
            m.bytes_transferred = b;
        }

        // Validate correctness
        let src_hash = metrics::hash_dir(&workload.source_dir);
        let dst_hash = metrics::hash_dir(dest_dir);
        m.correct = src_hash == dst_hash;
        if !m.correct {
            m.notes = format!("Hash mismatch: src={src_hash} dst={dst_hash}");
        }

        m.finalize();
        Ok(())
    }

    fn update(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        m: &mut RunMetrics,
    ) -> Result<(), String> {
        // For delta benchmarks, rsync is run against v2 source onto the existing dest
        let v2 = workload
            .source_dir_v2
            .as_ref()
            .ok_or("No v2 source for delta benchmark")?;

        std::fs::create_dir_all(dest_dir).map_err(|e| e.to_string())?;

        let src = format!("{}/", v2.to_string_lossy());
        let dst = format!("{}/", dest_dir.to_string_lossy());

        let start = Instant::now();
        let mut child = Command::new("rsync")
            .args([
                "-a", "--stats", "--delete", // remove files not in source
                &src, &dst,
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to spawn rsync: {e}"))?;

        let pid = child.id();
        let mut monitor = ProcessMonitor::new(&[pid]);

        wait_child_with_timeout(&mut child, &mut monitor, DEFAULT_CHILD_TIMEOUT)
            .map_err(|e| format!("rsync update: {e}"))?;

        let elapsed = start.elapsed();
        let output = child.wait_with_output().map_err(|e| e.to_string())?;
        let (peak_rss, cpu_secs) = monitor.finish();

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = workload.total_bytes_v2.unwrap_or(workload.total_bytes_v1);
        m.bytes_transferred = m.source_bytes; // fallback
        m.peak_rss_bytes = peak_rss;
        m.cpu_seconds = cpu_secs;
        m.dest_size_bytes = metrics::dir_size(dest_dir);

        // Parse rsync --stats output for actual bytes sent
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(b) = parse_rsync_total_sent(&stdout) {
            m.bytes_transferred = b;
        }

        let src_hash = metrics::hash_dir(v2);
        let dst_hash = metrics::hash_dir(dest_dir);
        m.correct = src_hash == dst_hash;
        if !m.correct {
            m.notes = format!("Hash mismatch: src={src_hash} dst={dst_hash}");
        }

        m.finalize();
        Ok(())
    }
}

/// Parse "Total sent: NNN bytes" (or "sent NNN bytes") from rsync --stats output.
/// Falls back to "Total transferred file size" if present.
fn parse_rsync_total_sent(stdout: &str) -> Option<u64> {
    // macOS rsync:  "sent 65698 bytes  received 42 bytes ..."
    // GNU rsync:    "Total sent: 65,698"  or  "sent 65,698 bytes ..."
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("sent ") || trimmed.starts_with("Total sent:") {
            let cleaned: String = trimmed
                .chars()
                .skip_while(|c| !c.is_ascii_digit())
                .take_while(|c| c.is_ascii_digit() || *c == ',')
                .filter(|c| c.is_ascii_digit())
                .collect();
            if let Ok(b) = cleaned.parse::<u64>() {
                if b > 0 {
                    return Some(b);
                }
            }
        }
    }
    // Fallback: "Total transferred file size: NNN"
    for line in stdout.lines() {
        if line.contains("Total transferred file size") {
            if let Some(bytes_str) = line.split(':').nth(1) {
                let cleaned: String = bytes_str.chars().filter(|c| c.is_ascii_digit()).collect();
                if let Ok(b) = cleaned.parse::<u64>() {
                    return Some(b);
                }
            }
        }
    }
    None
}
