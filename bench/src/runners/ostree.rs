//! ostree runner — uses `ostree` to commit and checkout a directory tree.
//! This measures the local commit + checkout lifecycle since ostree is primarily
//! a local content-addressed store, not a client-server transfer tool.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use crate::metrics::{self, ProcessMonitor, RunMetrics};
use crate::workload::Workload;

use super::ToolRunner;

pub struct OstreeRunner {
    work_dir: PathBuf,
    repo_dir: PathBuf,
}

impl OstreeRunner {
    pub fn new(work_dir: &Path) -> Self {
        let repo_dir = work_dir.join("ostree").join("repo");
        Self {
            work_dir: work_dir.join("ostree"),
            repo_dir,
        }
    }

    fn init_repo(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.repo_dir).map_err(|e| e.to_string())?;
        let status = Command::new("ostree")
            .args([
                "init",
                "--repo",
                &self.repo_dir.to_string_lossy(),
                "--mode=bare-user",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .status()
            .map_err(|e| format!("ostree init failed: {e}"))?;

        if !status.success() {
            return Err("ostree init failed".to_string());
        }
        Ok(())
    }

    fn commit(&self, source_dir: &Path, branch: &str) -> Result<String, String> {
        let output = Command::new("ostree")
            .args([
                "commit",
                "--repo",
                &self.repo_dir.to_string_lossy(),
                "--branch",
                branch,
                &source_dir.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .map_err(|e| format!("ostree commit failed: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("ostree commit failed: {stderr}"));
        }
        let commit_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(commit_id)
    }
}

impl ToolRunner for OstreeRunner {
    fn name(&self) -> &str {
        "ostree"
    }

    fn is_available(&self) -> bool {
        super::which("ostree")
    }

    fn transfer(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        m: &mut RunMetrics,
    ) -> Result<(), String> {
        // Clean any previous repo
        let _ = std::fs::remove_dir_all(&self.work_dir);
        self.init_repo()?;

        // Commit source into the repo
        self.commit(&workload.source_dir, "bench")?;

        // Checkout — this is the "transfer" measurement
        std::fs::create_dir_all(dest_dir).map_err(|e| e.to_string())?;

        // If dest_dir exists and is non-empty, remove it first (ostree checkout wants a fresh dir)
        if dest_dir.exists() {
            let _ = std::fs::remove_dir_all(dest_dir);
        }

        let start = Instant::now();
        let mut child = Command::new("ostree")
            .args([
                "checkout",
                "--repo",
                &self.repo_dir.to_string_lossy(),
                "bench",
                &dest_dir.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("ostree checkout failed to spawn: {e}"))?;

        let pid = child.id();
        let mut monitor = ProcessMonitor::new(&[pid]);

        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => {
                    monitor.sample();
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => return Err(format!("ostree wait error: {e}")),
            }
        }

        let elapsed = start.elapsed();
        let output = child.wait_with_output().map_err(|e| e.to_string())?;
        let (peak_rss, cpu_secs) = monitor.finish();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("ostree checkout failed: {stderr}"));
        }

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = workload.total_bytes_v1;
        // For ostree, "bytes transferred" is the repo object store size
        m.bytes_transferred = metrics::dir_size(&self.repo_dir);
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

        // Commit v2 (repo already initialized and has v1)
        self.commit(v2, "bench")?;

        // Remove existing checkout and re-checkout
        if dest_dir.exists() {
            let _ = std::fs::remove_dir_all(dest_dir);
        }

        let start = Instant::now();
        let mut child = Command::new("ostree")
            .args([
                "checkout",
                "--repo",
                &self.repo_dir.to_string_lossy(),
                "bench",
                &dest_dir.to_string_lossy(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("ostree checkout v2 failed: {e}"))?;

        let pid = child.id();
        let mut monitor = ProcessMonitor::new(&[pid]);

        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => {
                    monitor.sample();
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => return Err(format!("ostree wait error: {e}")),
            }
        }

        let elapsed = start.elapsed();
        let output = child.wait_with_output().map_err(|e| e.to_string())?;
        let (peak_rss, cpu_secs) = monitor.finish();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("ostree checkout v2 failed: {stderr}"));
        }

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = workload.total_bytes_v2.unwrap_or(workload.total_bytes_v1);
        m.bytes_transferred = metrics::dir_size(&self.repo_dir);
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
