use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Collected metrics for a single benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetrics {
    /// Which tool produced this result.
    pub tool: String,
    /// Workload name.
    pub workload: String,
    /// Applied benchmark network profile.
    pub network_profile: String,
    /// Iteration number.
    pub iteration: u32,
    /// Cache state: "cold" or "warm".
    pub cache_state: String,
    /// Whether this was a resume run.
    pub is_resume: bool,

    // Timing
    /// Wall-clock duration of the transfer/sync operation.
    pub wall_clock_secs: f64,
    /// Throughput in MiB/s (source bytes / wall_clock).
    pub throughput_mibs: f64,

    // Data
    /// Total source bytes (the artifact being transferred).
    pub source_bytes: u64,
    /// Bytes actually transferred on the wire (if measurable, else same as source_bytes).
    pub bytes_transferred: u64,

    // Resources
    /// Peak RSS growth in bytes during the operation, relative to the initial sample.
    pub peak_rss_bytes: u64,
    /// Approximate CPU-seconds consumed.
    pub cpu_seconds: f64,

    // Disk/store
    /// Destination directory size after transfer.
    pub dest_size_bytes: u64,
    /// Store overhead: dest_size - source_bytes (negative means dedup savings).
    pub store_overhead_bytes: i64,

    // Validation
    /// Whether the transferred data passed correctness checks.
    pub correct: bool,
    /// Optional notes (e.g. error messages, warnings).
    pub notes: String,
}

impl RunMetrics {
    pub fn new(
        tool: &str,
        workload: &str,
        network_profile: &str,
        iteration: u32,
        cache_state: &str,
    ) -> Self {
        Self {
            tool: tool.to_string(),
            workload: workload.to_string(),
            network_profile: network_profile.to_string(),
            iteration,
            cache_state: cache_state.to_string(),
            is_resume: false,
            wall_clock_secs: 0.0,
            throughput_mibs: 0.0,
            source_bytes: 0,
            bytes_transferred: 0,
            peak_rss_bytes: 0,
            cpu_seconds: 0.0,
            dest_size_bytes: 0,
            store_overhead_bytes: 0,
            correct: false,
            notes: String::new(),
        }
    }

    /// Compute derived fields after raw measurements are filled in.
    pub fn finalize(&mut self) {
        if self.wall_clock_secs > 0.0 {
            self.throughput_mibs =
                (self.source_bytes as f64) / (1024.0 * 1024.0) / self.wall_clock_secs;
        }
        self.store_overhead_bytes = self.dest_size_bytes as i64 - self.source_bytes as i64;
    }
}

/// Tracks resource usage of one or more OS processes over time.
pub struct ProcessMonitor {
    pids: Vec<Pid>,
    system: System,
    baseline_rss: u64,
    peak_rss_delta: u64,
    start: Instant,
    cpu_time_start: f64,
}

impl ProcessMonitor {
    pub fn new(pids: &[u32]) -> Self {
        let mut system = System::new();
        let pids: Vec<Pid> = pids.iter().map(|&p| Pid::from_u32(p)).collect();

        // Initial refresh — sysinfo 0.32 uses refresh_processes_specifics
        let refresh = ProcessRefreshKind::new().with_memory().with_cpu();
        system.refresh_processes_specifics(ProcessesToUpdate::Some(&pids), true, refresh);

        let baseline_rss: u64 = pids
            .iter()
            .filter_map(|pid| system.process(*pid))
            .map(|p| p.memory())
            .sum();

        let cpu_start: f64 = pids
            .iter()
            .filter_map(|pid| system.process(*pid))
            .map(|p| p.cpu_usage() as f64)
            .sum();

        Self {
            pids,
            system,
            baseline_rss,
            peak_rss_delta: 0,
            start: Instant::now(),
            cpu_time_start: cpu_start,
        }
    }

    /// Sample current process metrics. Call periodically during the benchmark.
    pub fn sample(&mut self) {
        let refresh = ProcessRefreshKind::new().with_memory().with_cpu();
        self.system
            .refresh_processes_specifics(ProcessesToUpdate::Some(&self.pids), true, refresh);

        let rss: u64 = self
            .pids
            .iter()
            .filter_map(|pid| self.system.process(*pid))
            .map(|p| p.memory())
            .sum();
        let rss_delta = rss.saturating_sub(self.baseline_rss);
        self.peak_rss_delta = self.peak_rss_delta.max(rss_delta);
    }

    /// Finalize and return (peak_rss_growth_bytes, cpu_seconds).
    pub fn finish(&mut self) -> (u64, f64) {
        self.sample();
        let elapsed = self.start.elapsed().as_secs_f64();
        // sysinfo reports cumulative cpu_usage as a percentage of one core
        let cpu_now: f64 = self
            .pids
            .iter()
            .filter_map(|pid| self.system.process(*pid))
            .map(|p| p.cpu_usage() as f64)
            .sum();
        // Rough approximation: average CPU% × elapsed time
        let avg_cpu_pct = (cpu_now + self.cpu_time_start) / 2.0;
        let cpu_seconds = avg_cpu_pct / 100.0 * elapsed;

        (self.peak_rss_delta, cpu_seconds)
    }
}

/// Compute total size of a directory tree (following symlinks).
pub fn dir_size(path: &Path) -> u64 {
    if path.is_file() {
        return std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    }
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else {
                total += std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

/// Attempt to drop filesystem caches on macOS. Requires root or will silently fail.
pub fn drop_caches() {
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("purge").status();
    }
    #[cfg(target_os = "linux")]
    {
        let _ = std::fs::write("/proc/sys/vm/drop_caches", "3");
        let _ = std::process::Command::new("sync").status();
    }
}

/// BLAKE3 hash of a file for correctness validation.
pub fn hash_file(path: &Path) -> String {
    use std::io::Read;
    let mut f = std::fs::File::open(path).expect("open for hash");
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 1024 * 1024];
    loop {
        let n = f.read(&mut buf).expect("read for hash");
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    hasher.finalize().to_hex().to_string()
}

/// BLAKE3 hash of an entire directory tree (sorted for determinism).
pub fn hash_dir(path: &Path) -> String {
    let mut hasher = blake3::Hasher::new();
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    collect_files(path, &mut files);
    files.sort();
    for file in &files {
        let rel = file.strip_prefix(path).unwrap_or(file);
        hasher.update(rel.to_string_lossy().as_bytes());
        let file_hash = hash_file(file);
        hasher.update(file_hash.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn collect_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    if dir.is_file() {
        out.push(dir.to_path_buf());
        return;
    }
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                collect_files(&p, out);
            } else {
                out.push(p);
            }
        }
    }
}
