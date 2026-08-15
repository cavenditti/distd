//! distd runner — spawns actual `distd_server` and `distd_client` binaries.
//!
//! The runner:
//! 1. Starts a server (with HashMapStorage or FsStorage depending on mode).
//! 2. Publishes the workload artifact via HTTP REST API.
//! 3. Spawns the client CLI to fetch/sync the artifact.
//! 4. Measures wall-clock, bytes, resources.
//!
//! Both "in-memory" (current default) and "persistent" server modes are supported.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use distd_core::{
    chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage},
    chunks::{ChunkAlgorithm, ChunkingPolicy},
    signature::{PublisherReleaseSequence, PublisherReleaseStatement},
};
use ring::signature::{Ed25519KeyPair, KeyPair};

use crate::metrics::{self, ProcessMonitor, RunMetrics};
use crate::network::{NetworkProfile, UdpShaperProxy};
use crate::workload::{self, Workload, WorkloadKind};

use super::{
    wait_child_with_timeout, RunnerOptions, ToolRunner, DEFAULT_CHILD_TIMEOUT, SMOKE_CHILD_TIMEOUT,
};

/// Timeout for curl HTTP requests to the server (connect + total).
const CURL_CONNECT_TIMEOUT: &str = "5";
const CURL_MAX_TIME: &str = "30";
const PUBLISH_RETRIES: usize = 3;
const HTTP_SERVER_ADDR: &str = "127.0.0.1:3000";
const SYNC_SERVER_ADDR: &str = "127.0.0.1:50051";
const BENCH_AUTH_TOKEN: &str = "distd-isolated-benchmark-token";
const BENCH_PUBLISHER_SEED: [u8; 32] = [0x42; 32];

fn benchmark_publisher_key_pair() -> Ed25519KeyPair {
    Ed25519KeyPair::from_seed_unchecked(&BENCH_PUBLISHER_SEED)
        .expect("fixed benchmark publisher seed is valid")
}

fn benchmark_publisher_public_key() -> String {
    benchmark_publisher_key_pair()
        .public_key()
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn benchmark_publisher_signature(
    item: &distd_core::item::Item,
    sequence: PublisherReleaseSequence,
) -> Result<String, String> {
    let statement = PublisherReleaseStatement::from_item(item, sequence)
        .map_err(|error| format!("cannot build benchmark publisher statement: {error}"))?;
    Ok(benchmark_publisher_key_pair()
        .sign(statement.as_bytes())
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

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

    fn stop_and_collect_output(&mut self) -> String {
        let Some(mut child) = self.child.take() else {
            return String::new();
        };
        let _ = child.kill();
        match child.wait_with_output() {
            Ok(output) => format!(
                "--- stdout ---\n{}\n--- stderr ---\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            ),
            Err(error) => format!("cannot collect distd server output: {error}"),
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let had_child = self.child.is_some();
        if let Some(ref mut child) = self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        // Belt-and-suspenders: also signal by PID in case the handle was consumed
        #[cfg(unix)]
        if had_child {
            unsafe {
                libc::kill(self.pid as i32, libc::SIGKILL);
            }
        }
    }
}

pub struct DistdRunner {
    work_dir: PathBuf,
    artifacts_dir: PathBuf,
    display_name: String,
    network: Option<NetworkProfile>,
    low_delta_chunking: String,
    corpus_seed: Option<u64>,
    server_guard: Mutex<Option<ServerGuard>>,
    sync_udp_proxy: Mutex<Option<UdpShaperProxy>>,
    publisher_sequences: Mutex<HashMap<String, u64>>,
}

impl DistdRunner {
    fn client_cache_dir(&self) -> PathBuf {
        self.work_dir.join("client-cache")
    }

    fn reset_client_cache(&self) -> Result<(), String> {
        let cache = self.client_cache_dir();
        if cache.exists() {
            std::fs::remove_dir_all(&cache).map_err(|error| error.to_string())?;
        }
        std::fs::create_dir_all(cache).map_err(|error| error.to_string())
    }

    fn http_version_ready() -> bool {
        let Ok(mut stream) = TcpStream::connect_timeout(
            &HTTP_SERVER_ADDR.parse().unwrap(),
            Duration::from_millis(200),
        ) else {
            return false;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
        let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
        if stream
            .write_all(b"GET /version HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .is_err()
        {
            return false;
        }

        let mut response = String::new();
        if stream.read_to_string(&mut response).is_err() {
            return false;
        }

        response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200")
    }

    pub fn new(work_dir: &Path, options: RunnerOptions) -> Self {
        Self {
            work_dir: work_dir.join("distd"),
            artifacts_dir: options.artifacts_dir.join("distd"),
            display_name: "distd-quic".to_string(),
            network: options.network,
            low_delta_chunking: options.distd_low_delta_chunking,
            corpus_seed: options.corpus_seed,
            server_guard: Mutex::new(None),
            sync_udp_proxy: Mutex::new(None),
            publisher_sequences: Mutex::new(HashMap::new()),
        }
    }

    fn next_publisher_sequence(
        &self,
        artifact_id: &str,
    ) -> Result<PublisherReleaseSequence, String> {
        let mut sequences = self.publisher_sequences.lock().unwrap();
        let next = sequences
            .get(artifact_id)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| "benchmark publisher release sequence exhausted".to_string())?;
        sequences.insert(artifact_id.to_owned(), next);
        Ok(PublisherReleaseSequence::new(next))
    }

    fn quic_port_ready() -> bool {
        std::net::UdpSocket::bind(SYNC_SERVER_ADDR).is_err()
    }

    /// Check if debug binaries exist in the workspace target directory.
    pub fn default_binary_paths_exist() -> bool {
        let workspace = workspace_root();
        let server = workspace.join("target/debug/distd_server");
        let client = workspace.join("target/debug/distd_client");
        server.exists() && client.exists()
    }

    fn binary_paths() -> Result<BinaryPaths, String> {
        let workspace = workspace_root();
        let paths = if cfg!(test) {
            BinaryPaths {
                server: workspace.join("target/debug/distd_server"),
                client: workspace.join("target/debug/distd_client"),
            }
        } else {
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
        };
        if cfg!(test) {
            for (binary, product) in [(&paths.server, "server"), (&paths.client, "client")] {
                let latest_source = latest_product_source_modification(&workspace, product)?;
                let modified = fs::metadata(binary)
                    .and_then(|metadata| metadata.modified())
                    .map_err(|error| {
                        format!(
                            "product binary {} is unavailable; build current debug binaries first: {error}",
                            binary.display()
                        )
                    })?;
                if modified < latest_source {
                    return Err(format!(
                        "product binary {} predates client/server/core sources; rebuild current debug binaries first",
                        binary.display()
                    ));
                }
            }
        }
        Ok(paths)
    }

    /// Start the distd server and wait for it to be ready.
    fn start_server(&self) -> Result<u32, String> {
        self.stop_server();

        // Make sure ports are free before starting
        if std::net::TcpStream::connect(HTTP_SERVER_ADDR).is_ok() {
            return Err("Port 3000 already in use before starting distd_server".to_string());
        }
        if std::net::UdpSocket::bind(SYNC_SERVER_ADDR).is_err() {
            return Err("Port 50051 already in use before starting distd_server".to_string());
        }

        std::fs::create_dir_all(&self.work_dir).map_err(|e| e.to_string())?;
        for entry in std::fs::read_dir(&self.work_dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            if entry.file_type().map_err(|e| e.to_string())?.is_dir()
                && entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("server_storage_")
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
        let server_state = self.work_dir.join("server-state");
        std::fs::create_dir_all(&server_state).map_err(|error| error.to_string())?;
        // Each benchmark iteration gets a new storage root, so every metadata
        // schema that can point at the previous root must be cleared as one
        // coordinated piece of ephemeral benchmark state. Keeping only the
        // obsolete bitcode path made iteration two load a valid selector for
        // storage that had deliberately been removed.
        for metadata_name in [
            "metadata.release.state",
            "metadata.signed.state",
            "metadata.state",
            "metadata.bitcode",
        ] {
            let metadata_path = server_state.join(metadata_name);
            match std::fs::remove_file(&metadata_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.to_string()),
            }
        }

        let bins = Self::binary_paths()?;
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
            .env("DISTD_SERVER_STATE_DIR", &server_state)
            .env("DISTD_CACHE_DIR", storage_root.join("cache"))
            .env("DISTD_ENROLLMENT_TOKEN", BENCH_AUTH_TOKEN)
            .env("DISTD_PUBLISH_TOKEN", BENCH_AUTH_TOKEN)
            .env("DISTD_ADMIN_TOKEN", BENCH_AUTH_TOKEN)
            .env(
                "DISTD_PUBLISHER_PUBLIC_KEYS",
                benchmark_publisher_public_key(),
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Ok(cache_mb) = std::env::var("DISTD_STORAGE_CHUNK_CACHE_MB") {
            cmd.env("DISTD_STORAGE_CHUNK_CACHE_MB", cache_mb);
        }
        if let Ok(tree_entries) = std::env::var("DISTD_STORAGE_TREE_CACHE_ENTRIES") {
            cmd.env("DISTD_STORAGE_TREE_CACHE_ENTRIES", tree_entries);
        }
        let child = cmd
            .spawn()
            .map_err(|e| format!("Failed to spawn distd_server: {e}"))?;

        let guard = ServerGuard::new(child);
        let pid = guard.pid();

        *self.server_guard.lock().unwrap() = Some(guard);

        // Wait for QUIC to bind its UDP port and HTTP to return a real 200 on /version.
        let start = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut http_ready = false;
        let mut quic_ready = false;
        loop {
            if start.elapsed() > timeout {
                let server_output = self.stop_server_and_collect_output();
                return Err(format!(
                    "Timed out waiting for distd_server (http_ready={http_ready}, quic_ready={quic_ready}): {server_output}"
                ));
            }
            if !http_ready {
                http_ready = Self::http_version_ready();
            }
            if !quic_ready {
                quic_ready = Self::quic_port_ready();
            }
            if http_ready && quic_ready {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        self.start_shapers()?;

        tracing::info!("distd_server started (pid {pid})");
        Ok(pid)
    }

    fn stop_server_and_collect_output(&self) -> String {
        self.stop_shapers();
        self.server_guard
            .lock()
            .unwrap()
            .take()
            .map(|mut guard| guard.stop_and_collect_output())
            .unwrap_or_default()
    }

    fn stop_server(&self) {
        let _ = self.stop_server_and_collect_output();
    }

    fn persist_operation_evidence(
        &self,
        metrics: &mut RunMetrics,
        client_stderr: &str,
        server_output: &str,
    ) -> Result<(), String> {
        std::fs::create_dir_all(&self.artifacts_dir).map_err(|error| {
            format!(
                "cannot create distd evidence directory {}: {error}",
                self.artifacts_dir.display()
            )
        })?;
        let evidence_dir = std::fs::canonicalize(&self.artifacts_dir).map_err(|error| {
            format!(
                "cannot resolve distd evidence directory {}: {error}",
                self.artifacts_dir.display()
            )
        })?;
        let stem = operation_evidence_stem(metrics);
        let client_path = evidence_dir.join(format!("{stem}--client-stderr.log"));
        let server_path = evidence_dir.join(format!("{stem}--server-output.log"));

        write_new_evidence_file(&client_path, client_stderr)?;
        write_new_evidence_file(&server_path, server_output)?;
        metrics.client_stderr_path = Some(client_path.to_string_lossy().into_owned());
        metrics.server_output_path = Some(server_path.to_string_lossy().into_owned());
        Ok(())
    }

    fn start_shapers(&self) -> Result<(), String> {
        self.stop_shapers();
        let Some(profile) = self.network.clone() else {
            return Ok(());
        };

        let sync_target: SocketAddr = SYNC_SERVER_ADDR.parse().unwrap();
        *self.sync_udp_proxy.lock().unwrap() = Some(UdpShaperProxy::start(sync_target, profile)?);

        Ok(())
    }

    fn stop_shapers(&self) {
        *self.sync_udp_proxy.lock().unwrap() = None;
    }

    fn publish_base_url(&self) -> String {
        format!("http://{HTTP_SERVER_ADDR}")
    }

    fn client_server_url(&self) -> String {
        self.sync_udp_proxy
            .lock()
            .unwrap()
            .as_ref()
            .map(|proxy| format!("quic://{}", proxy.listen_addr()))
            .unwrap_or_else(|| format!("quic://{SYNC_SERVER_ADDR}"))
    }

    fn selected_chunking_policy<'a>(&'a self, workload: &Workload) -> Option<&'a str> {
        (workload.kind == WorkloadKind::LowDeltaRevision)
            .then_some(self.low_delta_chunking.as_str())
    }

    fn chunk_algorithm(
        chunking_policy: Option<&str>,
        path: &Path,
        sample: &[u8],
        multi_file: bool,
    ) -> Result<ChunkAlgorithm, String> {
        let policy = chunking_policy
            .unwrap_or("auto")
            .parse::<ChunkingPolicy>()
            .map_err(|error| format!("invalid benchmark chunking policy: {error}"))?;
        Ok(ChunkAlgorithm::from_policy(
            policy, path, sample, multi_file,
        ))
    }

    fn signed_file_headers(
        source_file: &Path,
        item_name: &str,
        item_path: &str,
        chunking_policy: Option<&str>,
        sequence: PublisherReleaseSequence,
    ) -> Result<(String, String), String> {
        let data = std::fs::read(source_file)
            .map_err(|error| format!("cannot read benchmark publication input: {error}"))?;
        let algorithm = Self::chunk_algorithm(chunking_policy, source_file, &data, false)?;
        let item = HashMapStorage::default()
            .prepare_revision(
                item_name.to_string(),
                item_name.to_string(),
                PathBuf::from(item_path),
                0,
                None,
                Bytes::from(data),
                algorithm,
            )
            .map_err(|error| format!("cannot prepare signed benchmark publication: {error}"))?;
        Ok((
            benchmark_publisher_public_key(),
            benchmark_publisher_signature(&item, sequence)?,
        ))
    }

    fn signed_directory_headers(
        source_root: &Path,
        source_files: &[PathBuf],
        item_name: &str,
        item_path: &str,
        chunking_policy: Option<&str>,
        sequence: PublisherReleaseSequence,
    ) -> Result<(String, String), String> {
        let files = source_files
            .iter()
            .map(|source_file| {
                let relative = source_file.strip_prefix(source_root).map_err(|error| {
                    format!(
                        "cannot derive benchmark publication path for {}: {error}",
                        source_file.display()
                    )
                })?;
                let data = std::fs::read(source_file).map_err(|error| {
                    format!(
                        "cannot read benchmark publication input {}: {error}",
                        source_file.display()
                    )
                })?;
                Ok((relative.to_path_buf(), Bytes::from(data)))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let algorithm = Self::chunk_algorithm(chunking_policy, Path::new(item_path), &[], true)?;
        let item = HashMapStorage::default()
            .prepare_revision_from_files(
                item_name.to_string(),
                item_name.to_string(),
                PathBuf::from(item_path),
                0,
                None,
                files,
                algorithm,
            )
            .map_err(|error| format!("cannot prepare signed benchmark publication: {error}"))?;
        Ok((
            benchmark_publisher_public_key(),
            benchmark_publisher_signature(&item, sequence)?,
        ))
    }

    /// Publish a file to the running server via HTTP REST API.
    fn publish_file(
        &self,
        source_file: &Path,
        item_name: &str,
        item_path: &str,
        chunking_policy: Option<&str>,
    ) -> Result<(), String> {
        let sequence = self.next_publisher_sequence(item_name)?;
        let (publisher_key, publisher_signature) = Self::signed_file_headers(
            source_file,
            item_name,
            item_path,
            chunking_policy,
            sequence,
        )?;
        for attempt in 1..=PUBLISH_RETRIES {
            let publish_url = item_publish_url(
                &self.publish_base_url(),
                item_name,
                item_path,
                chunking_policy,
            );
            let output = Command::new("curl")
                .args([
                    "-sS",
                    "--fail-with-body",
                    "--connect-timeout",
                    CURL_CONNECT_TIMEOUT,
                    "--max-time",
                    CURL_MAX_TIME,
                    "-X",
                    "POST",
                    "-H",
                    &format!("Authorization: Bearer {BENCH_AUTH_TOKEN}"),
                    "-H",
                    "X-Distd-Publisher-Statement-Version: 2",
                    "-H",
                    &format!("X-Distd-Publisher-Release-Sequence: {}", sequence.get()),
                    "-H",
                    &format!("X-Distd-Publisher-Public-Key: {publisher_key}"),
                    "-H",
                    &format!("X-Distd-Publisher-Signature: {publisher_signature}"),
                    &publish_url,
                    "-F",
                    &format!("item=@{}", source_file.to_string_lossy()),
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .map_err(|e| format!("curl publish failed: {e}"))?;

            if output.status.success() {
                return Ok(());
            }

            if attempt == PUBLISH_RETRIES {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(format!(
                    "Failed to publish item to distd server: status={} stdout={} stderr={}",
                    output.status,
                    stdout.trim(),
                    stderr.trim()
                ));
            }

            std::thread::sleep(Duration::from_millis(150 * attempt as u64));
        }

        Ok(())
    }

    fn publish_files(
        &self,
        source_root: &Path,
        source_files: &[PathBuf],
        item_name: &str,
        item_path: &str,
        chunking_policy: Option<&str>,
    ) -> Result<(), String> {
        let sequence = self.next_publisher_sequence(item_name)?;
        let (publisher_key, publisher_signature) = Self::signed_directory_headers(
            source_root,
            source_files,
            item_name,
            item_path,
            chunking_policy,
            sequence,
        )?;
        for attempt in 1..=PUBLISH_RETRIES {
            let mut cmd = Command::new("curl");
            cmd.arg("-sS")
                .arg("--fail-with-body")
                .arg("--connect-timeout")
                .arg(CURL_CONNECT_TIMEOUT)
                .arg("--max-time")
                .arg(CURL_MAX_TIME)
                .arg("-X")
                .arg("POST")
                .arg("-H")
                .arg(format!("Authorization: Bearer {BENCH_AUTH_TOKEN}"))
                .arg("-H")
                .arg("X-Distd-Publisher-Statement-Version: 2")
                .arg("-H")
                .arg(format!(
                    "X-Distd-Publisher-Release-Sequence: {}",
                    sequence.get()
                ))
                .arg("-H")
                .arg(format!("X-Distd-Publisher-Public-Key: {publisher_key}"))
                .arg("-H")
                .arg(format!(
                    "X-Distd-Publisher-Signature: {publisher_signature}"
                ))
                .arg(item_publish_url(
                    &self.publish_base_url(),
                    item_name,
                    item_path,
                    chunking_policy,
                ));

            for source_file in source_files {
                let relative = source_file.strip_prefix(source_root).map_err(|e| {
                    format!(
                        "Cannot derive relative path for {}: {e}",
                        source_file.display()
                    )
                })?;
                cmd.arg("-F").arg(format!(
                    "item=@{};filename={}",
                    source_file.to_string_lossy(),
                    relative.to_string_lossy()
                ));
            }

            let output = cmd
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .map_err(|e| format!("curl publish failed: {e}"))?;

            if output.status.success() {
                return Ok(());
            }

            if attempt == PUBLISH_RETRIES {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(format!(
                    "Failed to publish multi-file item to distd server: status={} stdout={} stderr={}",
                    output.status,
                    stdout.trim(),
                    stderr.trim()
                ));
            }

            std::thread::sleep(Duration::from_millis(150 * attempt as u64));
        }

        Ok(())
    }

    /// Run the distd client to fetch an item.
    fn client_get(&self, item_path: &str, dest_dir: &Path) -> Result<(Child, PathBuf), String> {
        self.client_get_with_options(item_path, dest_dir, &benchmark_publisher_public_key(), None)
    }

    fn client_get_with_options(
        &self,
        item_path: &str,
        dest_dir: &Path,
        publisher_public_key: &str,
        max_sync_memory_mib: Option<u64>,
    ) -> Result<(Child, PathBuf), String> {
        let bins = Self::binary_paths()?;
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
        let certificate = std::fs::read(self.work_dir.join("server-state/quic-cert.der"))
            .map_err(|error| format!("Cannot read benchmark server certificate: {error}"))?;
        let certificate_fingerprint = blake3::hash(&certificate).to_hex().to_string();
        let mut config = serde_json::json!({
            "fsstorage": {
                "enabled": true,
                "root": storage_dir,
            },
            "server": {
                "url": self.client_server_url(),
                "enrollment_token": BENCH_AUTH_TOKEN,
                "certificate_fingerprint": certificate_fingerprint,
                "allow_tofu": false,
                "publisher_public_keys": [publisher_public_key],
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
        if let Some(max_sync_memory_mib) = max_sync_memory_mib {
            config["client"]["max_sync_memory_mib"] = serde_json::Value::from(max_sync_memory_mib);
        }

        let config_path = client_dir.join("ClientSettings.json");
        std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap())
            .map_err(|e| e.to_string())?;

        let child = Command::new(&bins.client)
            .args(["get", item_path])
            .current_dir(&client_dir)
            .env("RUST_LOG", "distd_client=info")
            .env("DISTD_CACHE_DIR", self.client_cache_dir())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to spawn distd_client: {e}"))?;

        Ok((child, client_dir))
    }
}

impl ToolRunner for DistdRunner {
    fn name(&self) -> &str {
        &self.display_name
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
        self.reset_client_cache()?;
        let chunking_policy = self.selected_chunking_policy(workload);
        m.chunking_policy = chunking_policy.map(str::to_string);
        m.corpus_seed = self.corpus_seed;

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

        if let Err(e) = if source_files.len() > 1 {
            self.publish_files(
                &workload.source_dir,
                &source_files,
                item_name,
                item_path,
                chunking_policy,
            )
        } else {
            self.publish_file(&source_files[0], item_name, item_path, chunking_policy)
        } {
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
        let (mut client_child, _client_dir) =
            self.client_get(item_path, dest_dir).inspect_err(|_e| {
                self.stop_server();
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
        let mut client_stderr = String::new();
        if let Some(ref mut stderr) = client_child.stderr {
            let _ = stderr.read_to_string(&mut client_stderr);
        }

        match wait_result {
            Ok(_) => {
                if let Ok(status) = client_child.wait() {
                    if !status.success() {
                        tracing::warn!(
                            "distd client exited with error; preserving full stderr as evidence"
                        );
                        m.notes.push_str(" | client exited unsuccessfully");
                    }
                } else {
                    // Already reaped by wait_child_with_timeout
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
        m.peak_rss_bytes = peak_rss;
        m.cpu_seconds = cpu_secs;

        let dest_file = dest_dir.join(item_path);
        record_complete_hashes(
            m,
            &workload.source_dir,
            &source_files,
            &dest_file,
            "dest file not found",
        );

        // Check server store size via REST API (with timeout)
        if let Ok(store_size) = query_store_size() {
            m.notes
                .push_str(&format!(" | server_store_bytes={store_size}"));
        }

        let server_output = self.stop_server_and_collect_output();
        apply_sync_diagnostics(m, &client_stderr, &server_output);
        self.persist_operation_evidence(m, &client_stderr, &server_output)?;
        m.finalize();
        Ok(())
    }

    fn update(
        &self,
        workload: &Workload,
        dest_dir: &Path,
        m: &mut RunMetrics,
    ) -> Result<(), String> {
        let chunking_policy = self.selected_chunking_policy(workload);
        m.chunking_policy = chunking_policy.map(str::to_string);
        m.corpus_seed = self.corpus_seed;
        let v2_dir = workload
            .source_dir_v2
            .as_ref()
            .ok_or("No v2 source for delta benchmark")?;
        let expected_source_bytes = workload.total_bytes_v2.unwrap_or(workload.total_bytes_v1);

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

        if v1_files.len() > 1 {
            self.publish_files(
                &workload.source_dir,
                &v1_files,
                item_name,
                item_path,
                chunking_policy,
            )
        } else {
            self.publish_file(&v1_files[0], item_name, item_path, chunking_policy)
        }
        .inspect_err(|_e| {
            self.stop_server();
        })?;

        std::fs::create_dir_all(dest_dir).map_err(|e| {
            self.stop_server();
            e.to_string()
        })?;
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
        if v2_files.is_empty() {
            self.stop_server();
            return Err("No files in v2 source".to_string());
        }
        if v2_files.len() > 1 {
            self.publish_files(v2_dir, &v2_files, item_name, item_path, chunking_policy)
        } else {
            self.publish_file(&v2_files[0], item_name, item_path, chunking_policy)
        }
        .inspect_err(|_e| {
            self.stop_server();
        })?;

        // --- Phase 3: fetch again (the measured delta operation) ---
        // The client's dest already has v1 data; this exercises the diff path
        let start = Instant::now();
        let (mut client_child, _client_dir) =
            self.client_get(item_path, dest_dir).inspect_err(|_e| {
                self.stop_server();
            })?;
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
        let mut client_stderr = String::new();
        if let Some(ref mut stderr) = client_child.stderr {
            let _ = stderr.read_to_string(&mut client_stderr);
        }

        match wait_result {
            Ok(_) => {
                if let Ok(status) = client_child.wait() {
                    if !status.success() {
                        tracing::warn!(
                            "distd client update exited with error; preserving full stderr as evidence"
                        );
                        m.notes.push_str(" | client exited unsuccessfully");
                    }
                    if client_stderr.contains("QUIC transport error") {
                        m.correct = false;
                        m.notes.push_str(" | transfer stream error observed");
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
        m.peak_rss_bytes = peak_rss;
        m.cpu_seconds = cpu_secs;

        record_complete_hashes(
            m,
            v2_dir,
            &v2_files,
            &dest_file,
            "dest file not found after update",
        );

        let server_output = self.stop_server_and_collect_output();
        apply_sync_diagnostics(m, &client_stderr, &server_output);
        if m.bytes_transferred >= expected_source_bytes {
            m.notes
                .push_str(" | update transferred full artifact or more");
        }
        self.persist_operation_evidence(m, &client_stderr, &server_output)?;
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

fn item_publish_url(
    base_url: &str,
    item_name: &str,
    item_path: &str,
    chunking_policy: Option<&str>,
) -> String {
    let mut url = format!("{base_url}/items?name={item_name}&path={item_path}");
    if let Some(policy) = chunking_policy {
        url.push_str("&chunking=");
        url.push_str(policy);
    }
    url
}

fn operation_evidence_stem(metrics: &RunMetrics) -> String {
    let operation = if metrics.workload.ends_with("-update") {
        "update"
    } else if metrics.cache_state == "setup" {
        "setup-full"
    } else {
        "full"
    };
    format!(
        "{}--{}--{}--iteration-{:03}",
        sanitize_evidence_component(&metrics.workload),
        operation,
        sanitize_evidence_component(&metrics.cache_state),
        metrics.iteration,
    )
}

fn sanitize_evidence_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn write_new_evidence_file(path: &Path, contents: &str) -> Result<(), String> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("cannot create evidence file {}: {error}", path.display()))?;
    file.write_all(contents.as_bytes())
        .map_err(|error| format!("cannot write evidence file {}: {error}", path.display()))
}

fn record_complete_hashes(
    metrics: &mut RunMetrics,
    source_root: &Path,
    source_files: &[PathBuf],
    destination: &Path,
    destination_missing_note: &str,
) {
    let expected_hash = if source_files.len() > 1 {
        metrics::hash_dir(source_root)
    } else {
        metrics::hash_file(&source_files[0])
    };
    metrics.expected_hash = Some(expected_hash.clone());

    if !destination.exists() {
        metrics.correct = false;
        metrics
            .notes
            .push_str(&format!(" | {destination_missing_note}"));
        return;
    }

    let actual_hash = if source_files.len() > 1 {
        metrics::hash_dir(destination)
    } else {
        metrics::hash_file(destination)
    };
    metrics.actual_hash = Some(actual_hash.clone());
    metrics.correct = expected_hash == actual_hash;
    if !metrics.correct {
        metrics.notes.push_str(&format!(
            " | hash mismatch: expected={expected_hash} actual={actual_hash}"
        ));
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ClientSyncDiagnostics {
    local_basis_chunks: Option<u64>,
    manifest_chunks: Option<u64>,
    received_chunks: Option<u64>,
    payload_bytes: Option<u64>,
    wire_bytes: Option<u64>,
    network_ms: Option<u64>,
    store_ms: Option<u64>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ServerSyncDiagnostics {
    manifest_chunks: Option<u64>,
    missing_chunks: Option<u64>,
}

fn parse_metric_value(output: &str, key: &str) -> Option<u64> {
    let prefix = format!("{key}=");
    output.lines().rev().find_map(|line| {
        line.split_whitespace().find_map(|token| {
            token
                .strip_prefix(&prefix)
                .and_then(|value| value.parse::<u64>().ok())
        })
    })
}

fn parse_client_sync_diagnostics(stderr: &str) -> ClientSyncDiagnostics {
    ClientSyncDiagnostics {
        local_basis_chunks: parse_metric_value(stderr, "distd_sync_local_basis_chunks"),
        manifest_chunks: parse_metric_value(stderr, "distd_sync_manifest_chunks"),
        received_chunks: parse_metric_value(stderr, "distd_sync_received_chunks"),
        payload_bytes: parse_metric_value(stderr, "distd_sync_payload_bytes"),
        wire_bytes: parse_metric_value(stderr, "distd_sync_wire_bytes"),
        network_ms: parse_metric_value(stderr, "distd_network_ms"),
        store_ms: parse_metric_value(stderr, "distd_store_ms"),
    }
}

fn parse_server_sync_diagnostics(output: &str) -> ServerSyncDiagnostics {
    ServerSyncDiagnostics {
        manifest_chunks: parse_metric_value(output, "distd_sync_manifest_chunks"),
        missing_chunks: parse_metric_value(output, "distd_sync_missing_chunks"),
    }
}

fn apply_sync_diagnostics(metrics: &mut RunMetrics, client_stderr: &str, server_output: &str) {
    let client = parse_client_sync_diagnostics(client_stderr);
    let server = parse_server_sync_diagnostics(server_output);

    metrics.local_basis_chunks = client.local_basis_chunks;
    metrics.manifest_chunks = client.manifest_chunks.or(server.manifest_chunks);
    metrics.server_missing_chunks = server.missing_chunks;
    metrics.received_chunks = client.received_chunks;
    metrics.payload_bytes = client.payload_bytes;
    metrics.wire_bytes = client.wire_bytes;
    metrics.client_network_ms = client.network_ms;
    metrics.client_store_ms = client.store_ms;
    metrics.bytes_transferred = metrics.wire_bytes.unwrap_or(0);

    for line in client_stderr
        .lines()
        .filter(|line| line.contains("distd_sync_local_basis_chunks="))
    {
        tracing::info!("    client: {}", line.trim());
    }
    for line in server_output
        .lines()
        .filter(|line| line.contains("distd_sync_missing_chunks="))
    {
        tracing::info!("    server: {}", line.trim());
    }

    let missing: Vec<&str> = [
        ("local_basis_chunks", metrics.local_basis_chunks),
        ("manifest_chunks", metrics.manifest_chunks),
        ("server_missing_chunks", metrics.server_missing_chunks),
        ("received_chunks", metrics.received_chunks),
        ("payload_bytes", metrics.payload_bytes),
        ("wire_bytes", metrics.wire_bytes),
        ("client_network_ms", metrics.client_network_ms),
        ("client_store_ms", metrics.client_store_ms),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.is_none().then_some(name))
    .collect();
    if !missing.is_empty() {
        metrics.notes.push_str(&format!(
            " | missing structured sync diagnostics: {}",
            missing.join(",")
        ));
    }

    if let (Some(client_count), Some(server_count)) =
        (client.manifest_chunks, server.manifest_chunks)
    {
        if client_count != server_count {
            metrics.notes.push_str(&format!(
                " | manifest chunk count mismatch: client={client_count} server={server_count}"
            ));
        }
    }
    if let (Some(missing_count), Some(received_count)) =
        (server.missing_chunks, client.received_chunks)
    {
        if missing_count != received_count {
            metrics.notes.push_str(&format!(
                " | missing/received chunk count mismatch: server={missing_count} client={received_count}"
            ));
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
            "-H",
            &format!("Authorization: Bearer {BENCH_AUTH_TOKEN}"),
            "http://127.0.0.1:3000/chunks/size-sum",
        ])
        .output()
        .map_err(|e| e.to_string())?;

    let body = String::from_utf8_lossy(&output.stdout);
    body.trim()
        .parse::<u64>()
        .map_err(|e| format!("Failed to parse store size: {e}"))
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

fn latest_product_source_modification(
    workspace: &Path,
    product: &str,
) -> Result<SystemTime, String> {
    fn visit(path: &Path, latest: &mut SystemTime) -> Result<(), String> {
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            format!("cannot inspect product source {}: {error}", path.display())
        })?;
        if metadata.is_dir() {
            for entry in fs::read_dir(path).map_err(|error| {
                format!(
                    "cannot enumerate product source {}: {error}",
                    path.display()
                )
            })? {
                visit(&entry.map_err(|error| error.to_string())?.path(), latest)?;
            }
        } else if metadata.is_file() {
            let modified = metadata.modified().map_err(|error| {
                format!(
                    "cannot read product source time {}: {error}",
                    path.display()
                )
            })?;
            *latest = (*latest).max(modified);
        } else {
            return Err(format!(
                "product source path is not a regular file or directory: {}",
                path.display()
            ));
        }
        Ok(())
    }

    let mut latest = UNIX_EPOCH;
    for relative in ["Cargo.toml", "core/Cargo.toml", "core/src", "core/proto"] {
        visit(&workspace.join(relative), &mut latest)?;
    }
    let product_paths: &[&str] = match product {
        "client" => &["client/Cargo.toml", "client/src"],
        "server" => &["server/Cargo.toml", "server/src"],
        _ => return Err(format!("unknown distd product binary {product}")),
    };
    for relative in product_paths {
        visit(&workspace.join(relative), &mut latest)?;
    }
    Ok(latest)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Mutex;

    use crate::metrics::RunMetrics;

    use super::{
        apply_sync_diagnostics, benchmark_publisher_public_key, item_publish_url,
        operation_evidence_stem, parse_client_sync_diagnostics, parse_server_sync_diagnostics,
        record_complete_hashes, write_new_evidence_file, ClientSyncDiagnostics, DistdRunner,
        ServerSyncDiagnostics,
    };
    use crate::runners::RunnerOptions;

    static PRODUCT_INTEGRATION_LOCK: Mutex<()> = Mutex::new(());

    fn decode_hex<const N: usize>(value: &str) -> [u8; N] {
        assert_eq!(value.len(), N * 2);
        let mut bytes = [0; N];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let digits = std::str::from_utf8(pair).unwrap();
            bytes[index] = u8::from_str_radix(digits, 16).unwrap();
        }
        bytes
    }

    #[test]
    fn benchmark_publications_use_the_configured_publisher_key_and_valid_signatures() {
        use bytes::Bytes;
        use distd_core::{
            chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage},
            chunks::ChunkAlgorithm,
            signature::{
                PublisherPublicKey, PublisherReleaseEnvelope, PublisherReleaseSequence,
                PublisherSignature,
            },
        };

        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("fixture.bin");
        let payload = b"signed benchmark publication";
        fs::write(&source, payload).unwrap();

        let sequence = PublisherReleaseSequence::new(1);
        let (key, signature) = DistdRunner::signed_file_headers(
            &source,
            "bench-artifact",
            "bench-artifact",
            None,
            sequence,
        )
        .unwrap();
        assert_eq!(key, benchmark_publisher_public_key());

        let item = HashMapStorage::default()
            .prepare_revision(
                "bench-artifact".to_string(),
                "bench-artifact".to_string(),
                "bench-artifact".into(),
                0,
                None,
                Bytes::copy_from_slice(payload),
                ChunkAlgorithm::from_policy("auto".parse().unwrap(), &source, payload, false),
            )
            .unwrap();
        let envelope = PublisherReleaseEnvelope::from_parts(
            PublisherPublicKey::from_bytes(decode_hex(&key)),
            PublisherSignature::from_bytes(decode_hex(&signature)),
        );
        envelope.verify(&item, sequence).unwrap();
    }

    fn integration_runner(root: &std::path::Path) -> DistdRunner {
        DistdRunner::new(
            root,
            RunnerOptions {
                network: None,
                artifacts_dir: root.join("artifacts"),
                distd_low_delta_chunking: "fixed".to_string(),
                corpus_seed: Some(17),
            },
        )
    }

    #[test]
    #[ignore = "requires built product binaries and exclusive loopback ports"]
    fn signed_quic_client_activates_file_delta_and_directory_artifacts() {
        let _exclusive = PRODUCT_INTEGRATION_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temporary = tempfile::tempdir().unwrap();
        let runner = integration_runner(temporary.path());
        runner.reset_client_cache().unwrap();
        runner.start_server().unwrap();

        let source = temporary.path().join("source.bin");
        let destination = temporary.path().join("destination");
        fs::create_dir_all(&destination).unwrap();
        let mut initial = (0..distd_core::chunks::CHUNK_SIZE * 3)
            .map(|index| ((index * 17) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&source, &initial).unwrap();
        runner
            .publish_file(
                &source,
                "bench-signed-file",
                "bench-signed-file",
                Some("fixed"),
            )
            .unwrap();
        let (client, _) = runner
            .client_get("bench-signed-file", &destination)
            .unwrap();
        let output = client.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read(destination.join("bench-signed-file")).unwrap(),
            initial
        );

        for byte in &mut initial[distd_core::chunks::CHUNK_SIZE..distd_core::chunks::CHUNK_SIZE * 2]
        {
            *byte ^= 0x5a;
        }
        fs::write(&source, &initial).unwrap();
        runner
            .publish_file(
                &source,
                "bench-signed-file",
                "bench-signed-file",
                Some("fixed"),
            )
            .unwrap();
        let (client, _) = runner
            .client_get("bench-signed-file", &destination)
            .unwrap();
        let output = client.wait_with_output().unwrap();
        let update_stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{update_stderr}");
        assert!(update_stderr.contains("distd_sync_local_basis_chunks=3"));
        assert!(update_stderr.contains("distd_sync_received_chunks=1"));
        assert_eq!(
            fs::read(destination.join("bench-signed-file")).unwrap(),
            initial
        );

        let directory_source = temporary.path().join("directory-source");
        fs::create_dir_all(directory_source.join("nested")).unwrap();
        fs::write(directory_source.join("a.bin"), b"signed directory a").unwrap();
        fs::write(directory_source.join("nested/b.bin"), b"signed directory b").unwrap();
        let source_files = crate::workload::walkdir_files(&directory_source);
        runner
            .publish_files(
                &directory_source,
                &source_files,
                "bench-signed-directory",
                "bench-signed-directory",
                Some("fixed"),
            )
            .unwrap();
        let (client, _) = runner
            .client_get("bench-signed-directory", &destination)
            .unwrap();
        let output = client.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read(destination.join("bench-signed-directory/a.bin")).unwrap(),
            b"signed directory a"
        );
        assert_eq!(
            fs::read(destination.join("bench-signed-directory/nested/b.bin")).unwrap(),
            b"signed directory b"
        );

        let server_output = runner.stop_server_and_collect_output();
        assert!(!server_output.contains("publisher authorization failed"));
        assert!(server_output.contains("distd_sync_missing_chunks=1"));
    }

    #[test]
    #[ignore = "requires built product binaries and exclusive loopback ports"]
    fn untrusted_quic_manifest_resets_before_possession_and_activation() {
        let _exclusive = PRODUCT_INTEGRATION_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temporary = tempfile::tempdir().unwrap();
        let runner = integration_runner(temporary.path());
        runner.reset_client_cache().unwrap();
        runner.start_server().unwrap();

        let source = temporary.path().join("untrusted-source.bin");
        let original = b"previously active signed publisher content";
        fs::write(&source, original).unwrap();
        runner
            .publish_file(&source, "bench-untrusted", "bench-untrusted", Some("fixed"))
            .unwrap();
        let destination = temporary.path().join("untrusted-destination");
        fs::create_dir_all(&destination).unwrap();
        let (client, _) = runner.client_get("bench-untrusted", &destination).unwrap();
        let output = client.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read(destination.join("bench-untrusted")).unwrap(),
            original
        );
        let durable_before = directory_snapshot(&destination);

        fs::write(
            &source,
            b"new content that must remain inactive for an untrusted client",
        )
        .unwrap();
        runner
            .publish_file(&source, "bench-untrusted", "bench-untrusted", Some("fixed"))
            .unwrap();
        let wrong_key = "11".repeat(32);
        let (client, _) = runner
            .client_get_with_options("bench-untrusted", &destination, &wrong_key, None)
            .unwrap();
        let output = client.wait_with_output().unwrap();
        assert!(
            !output.status.success(),
            "untrusted client unexpectedly succeeded; stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let client_error = String::from_utf8_lossy(&output.stderr);
        assert!(
            client_error.contains("PublisherAuthenticity(UntrustedPublisher)"),
            "unexpected client error: {client_error}"
        );
        assert_eq!(
            fs::read(destination.join("bench-untrusted")).unwrap(),
            original
        );
        assert_eq!(directory_snapshot(&destination), durable_before);

        let reopened =
            distd_core::chunk_storage::fs_storage::FsStorage::new(destination.clone()).unwrap();
        let active = reopened
            .open_active_revision(&destination.join("bench-untrusted"))
            .unwrap()
            .expect("previous signed revision remains active after restart");
        let mut restarted_bytes = Vec::new();
        use std::io::Read as _;
        active
            .open_file()
            .unwrap()
            .read_to_end(&mut restarted_bytes)
            .unwrap();
        assert_eq!(restarted_bytes, original);
        assert!(reopened.chunk_cache_stats().invariants_hold);

        let server_output = runner.stop_server_and_collect_output();
        assert_eq!(
            server_output.matches("distd_sync_missing_chunks=").count(),
            1,
            "only the valid seed fetch may send possession: {server_output}"
        );
    }

    #[test]
    #[ignore = "requires built product binaries and exclusive loopback ports"]
    fn over_budget_quic_manifest_preserves_active_bytes_and_rollback_watermark() {
        let _exclusive = PRODUCT_INTEGRATION_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temporary = tempfile::tempdir().unwrap();
        let runner = integration_runner(temporary.path());
        runner.reset_client_cache().unwrap();
        runner.start_server().unwrap();

        let source = temporary.path().join("budget-source.bin");
        let destination = temporary.path().join("budget-destination");
        fs::create_dir_all(&destination).unwrap();
        let original = vec![1u8; 512 * 1024];
        fs::write(&source, &original).unwrap();
        runner
            .publish_file(&source, "bench-budget", "bench-budget", Some("fixed"))
            .unwrap();
        let (client, _) = runner
            .client_get_with_options(
                "bench-budget",
                &destination,
                &benchmark_publisher_public_key(),
                Some(1),
            )
            .unwrap();
        let output = client.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read(destination.join("bench-budget")).unwrap(),
            original
        );

        let durable_before = directory_snapshot(&destination);
        let rollback_path = runner
            .client_cache_dir()
            .join("distd_client_rollback.state");
        let rollback_before = fs::read(&rollback_path).unwrap();

        let oversized = vec![2u8; 1024 * 1024 + 1];
        fs::write(&source, &oversized).unwrap();
        runner
            .publish_file(&source, "bench-budget", "bench-budget", Some("fixed"))
            .unwrap();
        let (client, _) = runner
            .client_get_with_options(
                "bench-budget",
                &destination,
                &benchmark_publisher_public_key(),
                Some(1),
            )
            .unwrap();
        let output = client.wait_with_output().unwrap();
        assert!(
            !output.status.success(),
            "over-budget client unexpectedly succeeded; stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let client_error = String::from_utf8_lossy(&output.stderr);
        assert!(
            client_error.contains("SyncMemoryLimit"),
            "unexpected client error: {client_error}"
        );
        assert_eq!(directory_snapshot(&destination), durable_before);
        assert_eq!(fs::read(&rollback_path).unwrap(), rollback_before);

        let reopened =
            distd_core::chunk_storage::fs_storage::FsStorage::new(destination.clone()).unwrap();
        let active = reopened
            .open_active_revision(&destination.join("bench-budget"))
            .unwrap()
            .expect("previous revision remains active after budget rejection");
        let mut restarted_bytes = Vec::new();
        use std::io::Read as _;
        active
            .open_file()
            .unwrap()
            .read_to_end(&mut restarted_bytes)
            .unwrap();
        assert_eq!(restarted_bytes, original);
        assert!(reopened.chunk_cache_stats().invariants_hold);
        drop(active);
        drop(reopened);

        let reopened_again =
            distd_core::chunk_storage::fs_storage::FsStorage::new(destination.clone()).unwrap();
        let active_again = reopened_again
            .open_active_revision(&destination.join("bench-budget"))
            .unwrap()
            .expect("previous revision remains active after a second budget-rejection restart");
        let mut twice_restarted_bytes = Vec::new();
        active_again
            .open_file()
            .unwrap()
            .read_to_end(&mut twice_restarted_bytes)
            .unwrap();
        assert_eq!(twice_restarted_bytes, original);
        assert!(reopened_again.chunk_cache_stats().invariants_hold);

        let server_output = runner.stop_server_and_collect_output();
        assert_eq!(
            server_output.matches("distd_sync_missing_chunks=").count(),
            1,
            "only the admitted seed fetch may send possession: {server_output}"
        );
    }

    fn directory_snapshot(root: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
        fn visit(
            root: &std::path::Path,
            current: &std::path::Path,
            snapshot: &mut Vec<(std::path::PathBuf, Vec<u8>)>,
        ) {
            let mut entries = fs::read_dir(current)
                .unwrap()
                .map(|entry| entry.unwrap())
                .collect::<Vec<_>>();
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries {
                let path = entry.path();
                let metadata = fs::symlink_metadata(&path).unwrap();
                if metadata.is_dir() {
                    visit(root, &path, snapshot);
                } else if metadata.is_file() {
                    snapshot.push((
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        fs::read(path).unwrap(),
                    ));
                }
            }
        }

        let mut snapshot = Vec::new();
        visit(root, root, &mut snapshot);
        snapshot
    }

    #[test]
    fn parses_complete_client_sync_diagnostics() {
        let stderr = "INFO distd_sync_local_basis_chunks=100 distd_sync_manifest_chunks=107 distd_sync_wire_bytes=1800000 distd_sync_payload_bytes=1750000 distd_sync_received_chunks=7 distd_network_ms=24 distd_store_ms=31";

        assert_eq!(
            parse_client_sync_diagnostics(stderr),
            ClientSyncDiagnostics {
                local_basis_chunks: Some(100),
                manifest_chunks: Some(107),
                received_chunks: Some(7),
                payload_bytes: Some(1_750_000),
                wire_bytes: Some(1_800_000),
                network_ms: Some(24),
                store_ms: Some(31),
            }
        );
    }

    #[test]
    fn parses_server_selection_diagnostics() {
        let output =
            "INFO distd_sync_manifest_chunks=107 distd_sync_missing_chunks=7 distd_sync_selection";

        assert_eq!(
            parse_server_sync_diagnostics(output),
            ServerSyncDiagnostics {
                manifest_chunks: Some(107),
                missing_chunks: Some(7),
            }
        );
    }

    #[test]
    fn applies_diagnostics_to_structured_run_metrics() {
        let client = "INFO distd_sync_local_basis_chunks=100 distd_sync_manifest_chunks=107 distd_sync_wire_bytes=1800000 distd_sync_payload_bytes=1750000 distd_sync_received_chunks=7 distd_network_ms=24 distd_store_ms=31";
        let server =
            "INFO distd_sync_manifest_chunks=107 distd_sync_missing_chunks=7 distd_sync_selection";
        let mut metrics = RunMetrics::new("distd-quic", "fixture", "default", 0, "warm");

        apply_sync_diagnostics(&mut metrics, client, server);

        assert_eq!(metrics.local_basis_chunks, Some(100));
        assert_eq!(metrics.manifest_chunks, Some(107));
        assert_eq!(metrics.server_missing_chunks, Some(7));
        assert_eq!(metrics.received_chunks, Some(7));
        assert_eq!(metrics.payload_bytes, Some(1_750_000));
        assert_eq!(metrics.wire_bytes, Some(1_800_000));
        assert_eq!(metrics.bytes_transferred, 1_800_000);
        assert_eq!(metrics.client_network_ms, Some(24));
        assert_eq!(metrics.client_store_ms, Some(31));
        assert!(metrics.notes.is_empty());
    }

    #[test]
    fn evidence_names_are_unique_by_operation_and_iteration_and_never_overwrite() {
        let full = RunMetrics::new("distd-quic", "low-delta-revision", "default", 2, "warm");
        let update = RunMetrics::new(
            "distd-quic",
            "low-delta-revision-update",
            "default",
            2,
            "warm",
        );
        let next = RunMetrics::new("distd-quic", "low-delta-revision", "default", 3, "warm");

        assert_ne!(
            operation_evidence_stem(&full),
            operation_evidence_stem(&update)
        );
        assert_ne!(
            operation_evidence_stem(&full),
            operation_evidence_stem(&next)
        );

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("evidence.log");
        write_new_evidence_file(&path, "complete first log\n").unwrap();
        assert!(write_new_evidence_file(&path, "replacement").is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), "complete first log\n");
    }

    #[test]
    fn records_complete_expected_and_actual_hashes() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        let destination = temp.path().join("destination.bin");
        fs::write(&source, b"complete benchmark content").unwrap();
        fs::write(&destination, b"complete benchmark content").unwrap();
        let mut metrics = RunMetrics::new("distd-quic", "fixture", "default", 0, "warm");

        record_complete_hashes(
            &mut metrics,
            temp.path(),
            std::slice::from_ref(&source),
            &destination,
            "destination missing",
        );

        assert!(metrics.correct);
        assert!(metrics.expected_hash.is_some());
        assert_eq!(metrics.expected_hash, metrics.actual_hash);
        assert_eq!(metrics.expected_hash.as_deref().unwrap().len(), 64);
    }

    #[test]
    fn low_delta_publication_uses_existing_explicit_chunking_query() {
        assert_eq!(
            item_publish_url(
                "http://127.0.0.1:3000",
                "bench-artifact",
                "bench-artifact",
                Some("fixed"),
            ),
            "http://127.0.0.1:3000/items?name=bench-artifact&path=bench-artifact&chunking=fixed"
        );
        assert_eq!(
            item_publish_url(
                "http://127.0.0.1:3000",
                "bench-artifact",
                "bench-artifact",
                None,
            ),
            "http://127.0.0.1:3000/items?name=bench-artifact&path=bench-artifact"
        );
    }
}
