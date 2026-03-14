//! Tool runners — each module knows how to drive one external tool or distd itself.

pub mod rsync;
pub mod zsync;
pub mod casync;
pub mod ostree;
pub mod distd;
pub mod http;

use std::str::FromStr;
use std::path::Path;
use std::process::Child;
use std::time::{Duration, Instant};

use crate::metrics::{ProcessMonitor, RunMetrics};
use crate::network::NetworkProfile;
use crate::workload::Workload;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistdTransport {
    Grpc,
    Quic,
}

impl DistdTransport {
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Grpc => "distd",
            Self::Quic => "distd-quic",
        }
    }
}

impl FromStr for DistdTransport {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "grpc" | "tcp" => Ok(Self::Grpc),
            "quic" | "udp" => Ok(Self::Quic),
            other => Err(format!("unsupported distd transport '{other}', expected grpc or quic")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RunnerOptions {
    pub distd_transport: DistdTransport,
    pub network: Option<NetworkProfile>,
}

impl RunnerOptions {
    pub fn network_label(&self) -> String {
        self.network
            .as_ref()
            .map(NetworkProfile::label)
            .unwrap_or_else(|| "default".to_string())
    }
}

/// Default per-child-process timeout: 5 minutes.
/// This prevents any single tool invocation from hanging the entire suite.
pub const DEFAULT_CHILD_TIMEOUT: Duration = Duration::from_secs(300);

/// Smoke-test timeout: 60 seconds (plenty for tiny workloads).
pub const SMOKE_CHILD_TIMEOUT: Duration = Duration::from_secs(60);

/// Wait for a child process with a timeout, sampling a `ProcessMonitor` while waiting.
/// Returns `Ok(elapsed)` on normal exit, `Err(msg)` on timeout (after killing the child).
pub fn wait_child_with_timeout(
    child: &mut Child,
    monitor: &mut ProcessMonitor,
    timeout: Duration,
) -> Result<Duration, String> {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(start.elapsed()),
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "Child process timed out after {:.0}s (limit: {:.0}s)",
                        start.elapsed().as_secs_f64(),
                        timeout.as_secs_f64(),
                    ));
                }
                monitor.sample();
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(format!("wait error: {e}")),
        }
    }
}

/// Trait implemented by each tool runner.
///
/// A `ToolRunner` knows how to:
/// 1. Prepare a server/source from a workload directory.
/// 2. Perform a sync/transfer to a destination.
/// 3. Clean up after itself.
pub trait ToolRunner: Send + Sync {
    /// Human-readable tool name (e.g. "rsync", "distd").
    fn name(&self) -> &str;

    /// Check whether the tool is installed and runnable.
    fn is_available(&self) -> bool;

    /// Perform a full transfer from the workload source to `dest_dir`.
    /// Returns partially-filled `RunMetrics`; caller fills iteration/cache metadata.
    fn transfer(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        metrics: &mut RunMetrics,
    ) -> Result<(), String>;

    /// Perform an incremental/delta update given that `dest_dir` already has v1.
    /// Default: just calls `transfer` with v2 source.
    fn update(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        metrics: &mut RunMetrics,
    ) -> Result<(), String> {
        self.transfer(workload, dest_dir, metrics)
    }

    /// Clean up any server-side state (e.g. kill spawned daemons).
    fn cleanup(&self) {}
}

/// All known tool names, in comparison order.
pub fn all_tool_names() -> &'static [&'static str] {
    &["distd", "rsync", "zsync", "http", "casync", "ostree"]
}

/// Check if a tool binary is available on PATH.
pub fn is_tool_available(name: &str) -> bool {
    match name {
        "distd" => {
            // Check for compiled binaries in workspace
            distd::DistdRunner::default_binary_paths_exist()
        }
        "rsync" => which("rsync"),
        "zsync" => which("zsync") || which("zsyncmake"),
        "http" => which("python3") && which("curl"),
        "casync" => which("casync"),
        "ostree" => which("ostree"),
        _ => false,
    }
}

/// Create a `ToolRunner` by name.
pub fn make_runner(name: &str, work_dir: &Path, options: &RunnerOptions) -> Option<Box<dyn ToolRunner>> {
    match name {
        "distd" => Some(Box::new(distd::DistdRunner::new(work_dir, options.clone()))),
        "rsync" => Some(Box::new(rsync::RsyncRunner::new(work_dir))),
        "zsync" => Some(Box::new(zsync::ZsyncRunner::new(work_dir))),
        "http" => Some(Box::new(http::HttpRunner::new(work_dir))),
        "casync" => Some(Box::new(casync::CasyncRunner::new(work_dir))),
        "ostree" => Some(Box::new(ostree::OstreeRunner::new(work_dir))),
        _ => None,
    }
}

fn which(cmd: &str) -> bool {
    std::process::Command::new("which")
        .arg(cmd)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
