//! distd runner — spawns actual `distd_server` and `distd_client` binaries.
//!
//! The runner:
//! 1. Starts a server (with HashMapStorage or FsStorage depending on mode).
//! 2. Publishes the workload artifact via HTTP REST API.
//! 3. Spawns the client CLI to fetch/sync the artifact.
//! 4. Measures wall-clock, bytes, resources.
//!
//! Both "in-memory" (current default) and "persistent" server modes are supported.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::metrics::{self, ProcessMonitor, RunMetrics};
use crate::workload::{self, Workload};

use super::{wait_child_with_timeout, ToolRunner, SMOKE_CHILD_TIMEOUT, DEFAULT_CHILD_TIMEOUT};

/// Timeout for curl HTTP requests to the server (connect + total).
const CURL_CONNECT_TIMEOUT: &str = "5";
const CURL_MAX_TIME: &str = "30";

/// Paths to the compiled distd binaries.
struct BinaryPaths {
    server: PathBuf,
    client: PathBuf,
}

/// RAII guard that kills the server on drop, ensuring cleanup even on early returns / panics.
struct ServerGuard {
    child: Option<Child>,
    pid: u32,
}

impl ServerGuard {
    fn new(child: Child) -> Self {
        let pid = child.id();
        Self {
            child: Some(child),
            pid,
        }
    }

    fn pid(&self) -> u32 {
        self.pid
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(ref mut child) = self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        // Belt-and-suspenders: also signal by PID in case the handle was consumed
        #[cfg(unix)]
        {
            unsafe {
                libc::kill(self.pid as i32, libc::SIGKILL);
            }
        }
    }
}

pub struct DistdRunner {
    work_dir: PathBuf,
    /// Server child held in a RefCell so `transfer(&self)` can manage it.
    server_guard: RefCell<Option<ServerGuard>>,
}

// SAFETY: DistdRunner is only used from one thread (the benchmark orchestrator
// runs tools sequentially). The RefCell is never shared across threads.
unsafe impl Send for DistdRunner {}
unsafe impl Sync for DistdRunner {}

impl DistdRunner {
    pub fn new(work_dir: &Path) -> Self {
        Self {
            work_dir: work_dir.join("distd"),
            server_guard: RefCell::new(None),
        }
    }

    /// Check if debug binaries exist in the workspace target directory.
    pub fn default_binary_paths_exist() -> bool {
        let workspace = workspace_root();
        let server = workspace.join("target/debug/distd_server");
        let client = workspace.join("target/debug/distd_client");
        server.exists() && client.exists()
    }

    fn binary_paths() -> BinaryPaths {
        let workspace = workspace_root();
        // Prefer release builds, fall back to debug
        let release_server = workspace.join("target/release/distd_server");
        let release_client = workspace.join("target/release/distd_client");
        if release_server.exists() && release_client.exists() {
            BinaryPaths {
                server: release_server,
                client: release_client,
            }
        } else {
            BinaryPaths {
                server: workspace.join("target/debug/distd_server"),
                client: workspace.join("target/debug/distd_client"),
            }
        }
    }

    /// Start the distd server and wait for it to be ready.
    fn start_server(&self) -> Result<u32, String> {
        self.stop_server();

        // Make sure ports are free before starting
        if std::net::TcpStream::connect("127.0.0.1:3000").is_ok() {
            return Err("Port 3000 already in use before starting distd_server".to_string());
        }
        if std::net::TcpStream::connect_timeout(
            &"127.0.0.1:50051".parse().unwrap(),
            Duration::from_millis(200),
        )
        .is_ok()
        {
            return Err("Port 50051 already in use before starting distd_server".to_string());
        }

        std::fs::create_dir_all(&self.work_dir).map_err(|e| e.to_string())?;
        for entry in std::fs::read_dir(&self.work_dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            if entry.file_type().map_err(|e| e.to_string())?.is_dir()
                && entry.file_name().to_string_lossy().starts_with("server_storage_")
            {
                std::fs::remove_dir_all(entry.path()).map_err(|e| e.to_string())?;
            }
        }
        let storage_nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos();
        let storage_root = self
            .work_dir
            .join(format!("server_storage_{storage_nonce}"));

        let bins = Self::binary_paths();
        if !bins.server.exists() {
            return Err(format!(
                "distd_server binary not found at {}. Run `cargo build` first.",
                bins.server.display()
            ));
        }

        let mut cmd = Command::new(&bins.server);
        cmd.current_dir(&self.work_dir)
            .env("RUST_LOG", "distd_server=info")
            .env("DISTD_STORAGE", "fs")
            .env("DISTD_STORAGE_ROOT", &storage_root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Ok(cache_mb) = std::env::var("DISTD_STORAGE_CHUNK_CACHE_MB") {
            cmd.env("DISTD_STORAGE_CHUNK_CACHE_MB", cache_mb);
        }
        let child = cmd
            .spawn()
            .map_err(|e| format!("Failed to spawn distd_server: {e}"))?;

        let guard = ServerGuard::new(child);
        let pid = guard.pid();

        *self.server_guard.borrow_mut() = Some(guard);

        // Wait for BOTH HTTP (3000) and gRPC (50051) ports to be ready.
        // The server binds gRPC on [::1]:50051 (IPv6) and HTTP on 0.0.0.0:3000.
        let start = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut http_ready = false;
        let mut grpc_ready = false;
        loop {
            if start.elapsed() > timeout {
                self.stop_server();
                return Err(format!(
                    "Timed out waiting for distd_server (http_ready={http_ready}, grpc_ready={grpc_ready})"
                ));
            }
            if !http_ready {
                http_ready = std::net::TcpStream::connect("127.0.0.1:3000").is_ok();
            }
            if !grpc_ready {
                grpc_ready = std::net::TcpStream::connect_timeout(
                    &"127.0.0.1:50051".parse().unwrap(),
                    Duration::from_millis(200),
                )
                .is_ok();
            }
            if http_ready && grpc_ready {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        tracing::info!("distd_server started (pid {pid})");
        Ok(pid)
    }

    fn stop_server(&self) {
        // Dropping the guard kills the child process
        *self.server_guard.borrow_mut() = None;
    }

    /// Publish a file to the running server via HTTP REST API.
    fn publish_file(
        &self,
        source_file: &Path,
        item_name: &str,
        item_path: &str,
    ) -> Result<(), String> {
        let output = Command::new("curl")
            .args([
                "-s",
                "-f",  // fail fast on HTTP errors
                "--connect-timeout",
                CURL_CONNECT_TIMEOUT,
                "--max-time",
                CURL_MAX_TIME,
                "-X",
                "POST",
                &format!(
                    "http://127.0.0.1:3000/items?name={}&path={}",
                    item_name, item_path
                ),
                "-F",
                &format!("item=@{}", source_file.to_string_lossy()),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| format!("curl publish failed: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Failed to publish item to distd server: {stderr}"));
        }
        Ok(())
    }

    /// Run the distd client to fetch an item.
    fn client_get(
        &self,
        item_path: &str,
        dest_dir: &Path,
    ) -> Result<(Child, PathBuf), String> {
        let bins = Self::binary_paths();
        if !bins.client.exists() {
            return Err(format!(
                "distd_client binary not found at {}.",
                bins.client.display()
            ));
        }

        // Create a client config file in the work directory
        let client_dir = self.work_dir.join("client_run");
        std::fs::create_dir_all(&client_dir).map_err(|e| e.to_string())?;

        let storage_dir = dest_dir.to_string_lossy().to_string();
        let config = serde_json::json!({
            "fsstorage": {
                "enabled": true,
                "root": storage_dir,
            },
            "server": {
                "url": "http://127.0.0.1:50051",
            },
            "log": {
                "level": "INFO",
            },
            "client": {
                "name": "bench-client",
                "sync": [item_path],
            },
            "debug": true,
        });

        let config_path = client_dir.join("ClientSettings.json");
        std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap())
            .map_err(|e| e.to_string())?;

        let child = Command::new(&bins.client)
            .args(["get", item_path])
            .current_dir(&client_dir)
            .env("RUST_LOG", "distd_client=info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to spawn distd_client: {e}"))?;

        Ok((child, client_dir))
    }
}

impl ToolRunner for DistdRunner {
    fn name(&self) -> &str {
        "distd"
    }

    fn is_available(&self) -> bool {
        Self::default_binary_paths_exist()
    }

    fn transfer(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        m: &mut RunMetrics,
    ) -> Result<(), String> {
        // Clear client cache to avoid stale UUIDs being rejected by fresh server
        // The client stores its UUID in ~/.cache/distd/ and the server rejects old UUIDs
        clean_distd_cache();

        // Start server (stop any previous one first)
        let server_pid = self.start_server()?;

        // Find source files and publish them
        let source_files = workload::walkdir_files(&workload.source_dir);
        if source_files.is_empty() {
            self.stop_server();
            return Err("No files in workload source".to_string());
        }

        let item_name = "bench-artifact";
        let item_path = "bench-artifact";

        // For multi-file workloads, tar the source directory into a single file
        // (matches distd's single-item model honestly)
        let publish_file = if source_files.len() > 1 {
            let tar_path = self.work_dir.join("publish_packed.tar");
            workload::pack_tar(&workload.source_dir, &tar_path);
            m.notes = format!(
                "distd: {} files tar-packed into single artifact",
                source_files.len()
            );
            tar_path
        } else {
            source_files[0].clone()
        };

        if let Err(e) = self.publish_file(&publish_file, item_name, item_path) {
            self.stop_server();
            return Err(e);
        }

        // Prepare dest
        std::fs::create_dir_all(dest_dir).map_err(|e| {
            self.stop_server();
            e.to_string()
        })?;

        // Fetch — this is the measured operation
        let start = Instant::now();
        let (mut client_child, _client_dir) = self.client_get(item_path, dest_dir).map_err(|e| {
            self.stop_server();
            e
        })?;
        let client_pid = client_child.id();

        let mut monitor = ProcessMonitor::new(&[server_pid, client_pid]);

        // Use a shorter timeout for smoke tests
        let timeout = if workload.total_bytes_v1 < 1024 * 1024 {
            SMOKE_CHILD_TIMEOUT
        } else {
            DEFAULT_CHILD_TIMEOUT
        };

        let wait_result = wait_child_with_timeout(&mut client_child, &mut monitor, timeout);

        let elapsed = start.elapsed();
        let (peak_rss, cpu_secs) = monitor.finish();

        match wait_result {
            Ok(_) => {
                let mut stderr_buf = String::new();
                if let Some(ref mut stderr) = client_child.stderr {
                    use std::io::Read;
                    let _ = stderr.read_to_string(&mut stderr_buf);
                }
                if let Ok(status) = client_child.wait() {
                    if !status.success() {
                        tracing::warn!("distd client exited with error: {stderr_buf}");
                        m.notes.push_str(&format!(" | client error: {stderr_buf}"));
                    }
                } else {
                    // Already reaped by wait_child_with_timeout
                }

                if let Some(bytes) = extract_payload_bytes(&stderr_buf) {
                    m.bytes_transferred = bytes;
                } else {
                    m.notes.push_str(" | missing payload byte metric");
                }
            }
            Err(timeout_msg) => {
                tracing::error!("distd client timed out: {timeout_msg}");
                m.notes.push_str(&format!(" | TIMEOUT: {timeout_msg}"));
                m.correct = false;
            }
        }

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = workload.total_bytes_v1;
        m.dest_size_bytes = metrics::dir_size(dest_dir);
        if m.bytes_transferred == 0 {
            m.bytes_transferred = m.dest_size_bytes;
        }
        m.peak_rss_bytes = peak_rss;
        m.cpu_seconds = cpu_secs;

        // Validate correctness by comparing source and dest hashes
        let src_hash = metrics::hash_file(&publish_file);
        // distd writes the item under dest_dir at the item_path
        let dest_file = dest_dir.join(item_path);
        if dest_file.exists() {
            let dst_hash = metrics::hash_file(&dest_file);
            m.correct = src_hash == dst_hash;
            if !m.correct {
                m.notes
                    .push_str(&format!(" | hash mismatch: src={src_hash} dst={dst_hash}"));
            }
        } else {
            m.correct = false;
            m.notes.push_str(" | dest file not found");
        }

        // Check server store size via REST API (with timeout)
        if let Ok(store_size) = query_store_size() {
            m.notes
                .push_str(&format!(" | server_store_bytes={store_size}"));
        }

        self.stop_server();
        m.finalize();
        Ok(())
    }

    fn update(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        m: &mut RunMetrics,
    ) -> Result<(), String> {
        let v2_dir = workload
            .source_dir_v2
            .as_ref()
            .ok_or("No v2 source for delta benchmark")?;
        let expected_source_bytes = workload.total_bytes_v2.unwrap_or(workload.total_bytes_v1);

        // Clear client cache to avoid stale UUIDs
        clean_distd_cache();

        // Start server
        let server_pid = self.start_server()?;

        // --- Phase 1: publish v1 so the server has a base revision ---
        let v1_files = workload::walkdir_files(&workload.source_dir);
        if v1_files.is_empty() {
            self.stop_server();
            return Err("No files in v1 source".to_string());
        }

        let item_name = "bench-artifact";
        let item_path = "bench-artifact";

        let v1_publish_file = if v1_files.len() > 1 {
            let tar_path = self.work_dir.join("update_v1.tar");
            workload::pack_tar(&workload.source_dir, &tar_path);
            tar_path
        } else {
            v1_files[0].clone()
        };

        self.publish_file(&v1_publish_file, item_name, item_path)
            .map_err(|e| { self.stop_server(); e })?;

        std::fs::create_dir_all(dest_dir).map_err(|e| { self.stop_server(); e.to_string() })?;
        let dest_file = dest_dir.join(item_path);
        if !dest_file.exists() {
            self.stop_server();
            return Err(format!(
                "Update benchmark requires an existing v1 destination at {}",
                dest_file.display()
            ));
        }

        // --- Phase 2: publish v2 to the SAME item path (triggers revision bump) ---
        let v2_files = workload::walkdir_files(v2_dir);
        let v2_publish_file = if v2_files.len() > 1 {
            let tar_path = self.work_dir.join("update_v2.tar");
            workload::pack_tar(v2_dir, &tar_path);
            tar_path
        } else if v2_files.is_empty() {
            self.stop_server();
            return Err("No files in v2 source".to_string());
        } else {
            v2_files[0].clone()
        };

        self.publish_file(&v2_publish_file, item_name, item_path)
            .map_err(|e| { self.stop_server(); e })?;

        // --- Phase 3: fetch again (the measured delta operation) ---
        // The client's dest already has v1 data; this exercises the diff path
        let start = Instant::now();
        let (mut client_child, _client_dir) = self.client_get(item_path, dest_dir)
            .map_err(|e| { self.stop_server(); e })?;
        let client_pid = client_child.id();
        let mut monitor = ProcessMonitor::new(&[server_pid, client_pid]);

        let timeout = if workload.total_bytes_v2.unwrap_or(workload.total_bytes_v1) < 1024 * 1024 {
            SMOKE_CHILD_TIMEOUT
        } else {
            DEFAULT_CHILD_TIMEOUT
        };

        let wait_result = wait_child_with_timeout(&mut client_child, &mut monitor, timeout);
        let elapsed = start.elapsed();
        let (peak_rss, cpu_secs) = monitor.finish();

        match wait_result {
            Ok(_) => {
                let mut stderr_buf = String::new();
                if let Some(ref mut stderr) = client_child.stderr {
                    use std::io::Read;
                    let _ = stderr.read_to_string(&mut stderr_buf);
                }
                if let Ok(status) = client_child.wait() {
                    if !status.success() {
                        tracing::warn!("distd client update exited with error: {stderr_buf}");
                        m.notes.push_str(&format!(" | client error: {stderr_buf}"));
                    }
                    if stderr_buf.contains("gRPC stream error during transfer") {
                        m.correct = false;
                        m.notes.push_str(" | transfer stream error observed");
                    }

                    if let Some(bytes) = extract_payload_bytes(&stderr_buf) {
                        m.bytes_transferred = bytes;
                    } else {
                        m.notes.push_str(" | missing payload byte metric");
                    }
                }
            }
            Err(timeout_msg) => {
                tracing::error!("distd client update timed out: {timeout_msg}");
                m.notes.push_str(&format!(" | TIMEOUT: {timeout_msg}"));
                m.correct = false;
            }
        }

        m.wall_clock_secs = elapsed.as_secs_f64();
        m.source_bytes = expected_source_bytes;
        m.dest_size_bytes = metrics::dir_size(dest_dir);
        if m.bytes_transferred == 0 {
            m.bytes_transferred = m.dest_size_bytes;
            m.notes.push_str(" | fell back to dest-size byte estimate");
        }
        m.peak_rss_bytes = peak_rss;
        m.cpu_seconds = cpu_secs;

        if m.bytes_transferred >= expected_source_bytes {
            m.notes.push_str(" | update transferred full artifact or more");
        }

        // Validate v2 output hash
        if dest_file.exists() {
            let src_hash = metrics::hash_file(&v2_publish_file);
            let dst_hash = metrics::hash_file(&dest_file);
            m.correct = src_hash == dst_hash;
            if !m.correct {
                m.notes
                    .push_str(&format!(" | hash mismatch: src={src_hash} dst={dst_hash}"));
            }
        } else {
            m.correct = false;
            m.notes.push_str(" | dest file not found after update");
        }

        self.stop_server();
        m.finalize();
        Ok(())
    }

    fn cleanup(&self) {
        self.stop_server();
        let _ = std::fs::remove_dir_all(&self.work_dir);
    }
}

impl Drop for DistdRunner {
    fn drop(&mut self) {
        self.stop_server();
    }
}

/// Remove the distd client cache directory (UUIDs, FsStorage persistence, etc.)
/// so that each benchmark run starts with a clean slate.
fn clean_distd_cache() {
    // Cross-platform: use $XDG_CACHE_HOME or platform-specific default
    let cache_base = std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .ok()
        .or_else(|| {
            std::env::var("HOME").ok().map(|h| {
                let home = PathBuf::from(h);
                if cfg!(target_os = "macos") {
                    home.join("Library/Caches")
                } else {
                    home.join(".cache")
                }
            })
        });

    if let Some(base) = cache_base {
        let cache_path = base.join("distd");
        if cache_path.exists() {
            if let Err(e) = std::fs::remove_dir_all(&cache_path) {
                tracing::warn!(
                    "Failed to clean distd cache at {}: {e}",
                    cache_path.display()
                );
            }
        }
    }
}

/// Query the server's chunk store size via REST.
fn query_store_size() -> Result<u64, String> {
    let output = Command::new("curl")
        .args([
            "-s",
            "-f",
            "--connect-timeout",
            CURL_CONNECT_TIMEOUT,
            "--max-time",
            "5",
            "http://127.0.0.1:3000/chunks/size-sum",
        ])
        .output()
        .map_err(|e| e.to_string())?;

    let body = String::from_utf8_lossy(&output.stdout);
    body.trim()
        .parse::<u64>()
        .map_err(|e| format!("Failed to parse store size: {e}"))
}

fn extract_payload_bytes(stderr: &str) -> Option<u64> {
    stderr.lines().find_map(|line| {
        line.split("distd_payload_bytes=")
            .nth(1)
            .and_then(|value| value.trim().parse::<u64>().ok())
    })
}

/// Find the workspace root by walking up from the current executable or CARGO_MANIFEST_DIR.
fn workspace_root() -> PathBuf {
    // Try CARGO_MANIFEST_DIR first (works during `cargo run`)
    if let Ok(dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let manifest = PathBuf::from(dir);
        if let Some(parent) = manifest.parent() {
            return parent.to_path_buf();
        }
    }

    // Fall back to current exe location
    if let Ok(exe) = std::env::current_exe() {
        // Typically in target/debug/ or target/release/
        if let Some(target_dir) = exe.parent() {
            if let Some(workspace) = target_dir.parent().and_then(|p| p.parent()) {
                return workspace.to_path_buf();
            }
        }
    }

    // Last resort
    PathBuf::from(".")
}
