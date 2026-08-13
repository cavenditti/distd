use distd_core::chunk_storage::fs_storage::{FsStorage, FsStorageCacheConfig};
use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::feed::Feed;
use std::net::SocketAddr;

use crate::client::Client;
use crate::quic::QuicResourceSettings;
use crate::server::{InternalMetadata, Server};

pub mod client;
pub mod error;
pub mod quic;
pub mod rest_api;
pub mod server;

const DEFAULT_HTTP_BIND: &str = "127.0.0.1:3000";
const HTTP_BIND_ENV: &str = "DISTD_HTTP_BIND";
const ALLOW_INSECURE_REMOTE_HTTP_ENV: &str = "DISTD_ALLOW_INSECURE_REMOTE_HTTP";

fn parse_http_bind_policy(
    configured_bind: Option<&str>,
    configured_override: Option<&str>,
) -> Result<SocketAddr, String> {
    let allow_insecure_remote = match configured_override {
        None | Some("false") => false,
        Some("true") => true,
        Some(_) => {
            return Err(format!(
                "{ALLOW_INSECURE_REMOTE_HTTP_ENV} must be exactly \"true\" or \"false\""
            ));
        }
    };

    let bind = configured_bind.unwrap_or(DEFAULT_HTTP_BIND);
    if bind.is_empty() {
        return Err(format!(
            "{HTTP_BIND_ENV} must be a non-empty socket address such as {DEFAULT_HTTP_BIND}"
        ));
    }
    let address = bind.parse::<SocketAddr>().map_err(|_| {
        format!(
            "{HTTP_BIND_ENV} must be a valid IP socket address such as {DEFAULT_HTTP_BIND} or [::1]:3000"
        )
    })?;

    if !address.ip().is_loopback() && !allow_insecure_remote {
        return Err(format!(
            "refusing plaintext HTTP bind to non-loopback address {address}; bind {HTTP_BIND_ENV} to loopback or set {ALLOW_INSECURE_REMOTE_HTTP_ENV}=true to explicitly accept insecure remote HTTP"
        ));
    }

    Ok(address)
}

fn optional_utf8_environment(name: &str) -> Result<Option<String>, String> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{name} must contain valid UTF-8")),
    }
}

fn configured_http_bind() -> Result<SocketAddr, String> {
    let bind = optional_utf8_environment(HTTP_BIND_ENV)?;
    let allow_insecure_remote = optional_utf8_environment(ALLOW_INSECURE_REMOTE_HTTP_ENV)?;
    parse_http_bind_policy(bind.as_deref(), allow_insecure_remote.as_deref())
}

fn required_secret(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            eprintln!("{name} must be configured with a non-empty value");
            std::process::exit(2);
        })
}

fn load_metadata(path: &std::path::Path) -> InternalMetadata {
    InternalMetadata::load(path).unwrap_or_else(|error| {
        eprintln!("Cannot load server metadata: {error}");
        std::process::exit(2);
    })
}

/// Run the server with a concrete storage backend.
async fn run_server<T>(
    server: Server<T>,
    http_addr: SocketAddr,
    quic_settings: QuicResourceSettings,
) where
    T: ChunkStorage + Send + Sync + std::fmt::Debug + 'static,
{
    let feed = Feed::new("A feed");
    let _ = server.expose_feed(feed).await;

    let app = rest_api::make_app(server.clone());

    let addr_quic = "0.0.0.0:50051".parse().unwrap();
    tokio::spawn(async move {
        if let Err(err) = server
            .clone()
            .serve_quic_with_settings(addr_quic, quic_settings)
            .await
        {
            tracing::error!("QUIC server failed: {err}");
        }
    });
    tracing::info!("listening on {} for QUIC", addr_quic);

    let listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .unwrap_or_else(|error| {
            eprintln!("Cannot bind HTTP listener at {http_addr}: {error}");
            std::process::exit(2);
        });
    tracing::info!("listening on {} for HTTP", http_addr);

    axum::serve(listener, app).await.unwrap();
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    let http_addr = configured_http_bind().unwrap_or_else(|error| {
        eprintln!("Invalid HTTP bind configuration: {error}");
        std::process::exit(2);
    });
    let quic_settings = QuicResourceSettings::from_environment().unwrap_or_else(|error| {
        eprintln!("Invalid QUIC resource configuration: {error}");
        std::process::exit(2);
    });
    let enrollment_token = required_secret("DISTD_ENROLLMENT_TOKEN");
    let state_dir = std::env::var("DISTD_SERVER_STATE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| distd_core::utils::settings::cache_dir().join("server-state"));
    let metadata_path = state_dir.join("metadata.bitcode");

    // Select storage backend via DISTD_STORAGE env var.
    // Supported values: "fs", "memory" (default when redb feature is off), "redb" (default).
    let storage_kind = std::env::var("DISTD_STORAGE").unwrap_or_else(|_| {
        if cfg!(feature = "redb") {
            "redb".to_string()
        } else {
            "fs".to_string()
        }
    });

    match storage_kind.as_str() {
        "fs" => {
            let metadata = load_metadata(&metadata_path);
            let root = std::env::var("DISTD_STORAGE_ROOT")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| distd_core::utils::settings::cache_dir().join("server-fs"));
            let chunk_cache_mb = std::env::var("DISTD_STORAGE_CHUNK_CACHE_MB")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(FsStorageCacheConfig::default().max_chunk_bytes / (1024 * 1024));
            let tree_cache_entries = std::env::var("DISTD_STORAGE_TREE_CACHE_ENTRIES")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(FsStorageCacheConfig::default().max_tree_entries);
            tracing::info!(
                "Using filesystem storage at {} with {} MiB chunk cache and {} tree cache entries",
                root.display(),
                chunk_cache_mb,
                tree_cache_entries,
            );
            let storage = FsStorage::with_cache_config(
                root,
                FsStorageCacheConfig {
                    max_chunk_bytes: chunk_cache_mb.saturating_mul(1024 * 1024),
                    max_tree_entries: tree_cache_entries,
                },
            )
            .unwrap_or_else(|error| {
                eprintln!("Cannot initialize filesystem storage: {error}");
                std::process::exit(2);
            });
            let server = Server::new_ephemeral_with_metadata(storage, metadata.clone())
                .with_enrollment_token(enrollment_token.clone())
                .with_metadata_persistence(metadata_path.clone());
            run_server(server, http_addr, quic_settings).await;
        }
        #[cfg(feature = "redb")]
        "redb" => {
            let metadata = load_metadata(&metadata_path);
            let db_path = distd_core::utils::settings::cache_dir().join("server.redb");
            tracing::info!("Using redb storage at {}", db_path.display());
            let storage = distd_core::chunk_storage::redb::RedbStorage::new(&db_path)
                .expect("Failed to open redb database");
            let server = Server::new_ephemeral_with_metadata(storage, metadata.clone())
                .with_enrollment_token(enrollment_token.clone())
                .with_metadata_persistence(metadata_path.clone());
            run_server(server, http_addr, quic_settings).await;
        }
        "memory" => {
            tracing::info!("Using in-memory storage (ephemeral)");
            let server = Server::new_ephemeral(HashMapStorage::default())
                .with_enrollment_token(enrollment_token.clone());
            run_server(server, http_addr, quic_settings).await;
        }
        other => {
            eprintln!(
                "Unknown DISTD_STORAGE value: {other}. Supported: \"fs\", \"redb\", \"memory\"."
            );
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_http_bind_policy, ALLOW_INSECURE_REMOTE_HTTP_ENV, DEFAULT_HTTP_BIND, HTTP_BIND_ENV,
    };

    #[test]
    fn http_bind_defaults_to_ipv4_loopback() {
        let address = parse_http_bind_policy(None, None).expect("default bind");
        assert_eq!(address, DEFAULT_HTTP_BIND.parse().unwrap());
        assert!(address.ip().is_loopback());
    }

    #[test]
    fn explicit_ipv4_and_ipv6_loopback_binds_are_allowed() {
        for bind in ["127.0.0.1:4100", "[::1]:4100"] {
            let address = parse_http_bind_policy(Some(bind), None).expect("loopback bind");
            assert!(address.ip().is_loopback());
            assert_eq!(address.port(), 4100);
        }
    }

    #[test]
    fn wildcard_and_non_loopback_binds_fail_closed() {
        for bind in ["0.0.0.0:3000", "[::]:3000", "192.0.2.10:3000"] {
            let error = parse_http_bind_policy(Some(bind), None).expect_err("unsafe bind");
            assert!(error.contains("refusing plaintext HTTP bind"));
            assert!(error.contains(ALLOW_INSECURE_REMOTE_HTTP_ENV));
        }
    }

    #[test]
    fn only_exact_true_enables_insecure_remote_http() {
        for bind in ["0.0.0.0:3000", "[::]:3000", "192.0.2.10:3000"] {
            let address = parse_http_bind_policy(Some(bind), Some("true"))
                .expect("explicit insecure override");
            assert_eq!(address, bind.parse().unwrap());
        }

        let error = parse_http_bind_policy(Some("0.0.0.0:3000"), Some("false"))
            .expect_err("false must not enable remote HTTP");
        assert!(error.contains("refusing plaintext HTTP bind"));
    }

    #[test]
    fn empty_or_ambiguous_override_values_are_rejected_without_echoing_them() {
        for value in ["", "1", "yes", "TRUE", " true", "false ", "sensitive-value"] {
            let error = parse_http_bind_policy(None, Some(value)).expect_err("invalid override");
            assert_eq!(
                error,
                format!("{ALLOW_INSECURE_REMOTE_HTTP_ENV} must be exactly \"true\" or \"false\"")
            );
            if !value.is_empty() {
                assert!(!error.contains(value));
            }
        }
    }

    #[test]
    fn empty_and_invalid_bind_values_have_actionable_errors() {
        let empty = parse_http_bind_policy(Some(""), None).expect_err("empty bind");
        assert!(empty.contains(HTTP_BIND_ENV));
        assert!(empty.contains("non-empty socket address"));

        for bind in ["localhost:3000", "127.0.0.1", "127.0.0.1:99999"] {
            let error = parse_http_bind_policy(Some(bind), None).expect_err("invalid bind");
            assert!(error.contains(HTTP_BIND_ENV));
            assert!(error.contains("valid IP socket address"));
        }
    }
}
