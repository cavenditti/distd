//! casync runner — uses `casync make` to create a chunk store and index, then `casync extract`
//! to reconstruct.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use crate::metrics::{self, ProcessMonitor, RunMetrics};
use crate::workload::Workload;

use super::ToolRunner;

pub struct CasyncRunner {
    work_dir: PathBuf,
}

impl CasyncRunner {
    pub fn new(work_dir: &Path) -> Self {
        Self {
            work_dir: work_dir.join("casync"),
        }
    }
}

impl ToolRunner for CasyncRunner {
    fn name(&self) -> &str {
        "casync"
    }

    fn is_available(&self) -> bool {
        super::which("casync")
    }

    fn transfer(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        m: &mut RunMetrics,
    ) -> Result<(), String> {
        std::fs::create_dir_all(dest_dir).map_err(|e| e.to_string())?;
        let store_dir = self.work_dir.join("store");
        std::fs::create_dir_all(&store_dir).map_err(|e| e.to_string())?;

        let index_file = self.work_dir.join("workload.caidx");

        // casync make — create index + chunk store from source
        let make_status = Command::new("casync")
            .args([
                "make",
                "--store",
                &store_dir.to_string_lossy(),
                &index_file.to_string_lossy(),
                &workload.source_dir.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .status()
            .map_err(|e| format!("casync make failed: {e}"))?;

        if !make_status.success() {
            return Err("casync make exited with error".to_string());
        }

        // casync extract — reconstruct from index + store (this is the transfer measurement)
        let start = Instant::now();
        let mut child = Command::new("casync")
            .args([
                "extract",
                "--store",
                &store_dir.to_string_lossy(),
                &index_file.to_string_lossy(),
                &dest_dir.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("casync extract failed: {e}"))?;

        let pid = child.id();
        let mut monitor = ProcessMonitor::new(&[pid]);

        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => {
                    monitor.sample();
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => return Err(format!("casync wait error: {e}")),
            }
        }

        let elapsed = start.elapsed();
        let output = child.wait_with_output().map_err(|e| e.to_string())?;
        let (peak_rss, cpu_secs) = monitor.finish();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("casync extract failed: {stderr}"));
        }

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = workload.total_bytes_v1;
        // Store size is a better measure of what casync actually transfers
        m.bytes_transferred = metrics::dir_size(&store_dir);
        m.peak_rss_bytes = peak_rss;
        m.cpu_seconds = cpu_secs;
        m.dest_size_bytes = metrics::dir_size(dest_dir);

        // Validate
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
        let v2 = workload
            .source_dir_v2
            .as_ref()
            .ok_or("No v2 source for delta benchmark")?;

        let store_dir = self.work_dir.join("store_v2");
        std::fs::create_dir_all(&store_dir).map_err(|e| e.to_string())?;

        // Also keep the v1 store around for seed
        let _store_v1 = self.work_dir.join("store");
        let index_v2 = self.work_dir.join("workload_v2.caidx");

        // Make v2 index + store
        let make_status = Command::new("casync")
            .args([
                "make",
                "--store",
                &store_dir.to_string_lossy(),
                &index_v2.to_string_lossy(),
                &v2.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .status()
            .map_err(|e| format!("casync make v2 failed: {e}"))?;

        if !make_status.success() {
            return Err("casync make v2 exited with error".to_string());
        }

        // Extract v2 with seed from existing dest
        let start = Instant::now();
        let mut child = Command::new("casync")
            .args([
                "extract",
                "--store",
                &store_dir.to_string_lossy(),
                "--seed",
                &dest_dir.to_string_lossy(),
                &index_v2.to_string_lossy(),
                &dest_dir.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("casync extract v2 failed: {e}"))?;

        let pid = child.id();
        let mut monitor = ProcessMonitor::new(&[pid]);

        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => {
                    monitor.sample();
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => return Err(format!("casync wait error: {e}")),
            }
        }

        let elapsed = start.elapsed();
        let output = child.wait_with_output().map_err(|e| e.to_string())?;
        let (peak_rss, cpu_secs) = monitor.finish();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("casync extract v2 failed: {stderr}"));
        }

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = workload.total_bytes_v2.unwrap_or(workload.total_bytes_v1);
        m.bytes_transferred = metrics::dir_size(&store_dir);
        m.peak_rss_bytes = peak_rss;
        m.cpu_seconds = cpu_secs;
        m.dest_size_bytes = metrics::dir_size(dest_dir);

        let src_hash = metrics::hash_dir(v2);
        let dst_hash = metrics::hash_dir(dest_dir);
        m.correct = src_hash == dst_hash;
        if !m.correct {
            m.notes = format!("Delta hash mismatch: v2={src_hash} dst={dst_hash}");
        }

        m.finalize();
        Ok(())
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.work_dir);
    }
}
