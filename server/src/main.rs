use distd_core::chunk_storage::fs_storage::{FsStorage, FsStorageCacheConfig};
use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::feed::Feed;
use distd_core::signature::PublisherPublicKey;
use std::{collections::HashSet, net::SocketAddr, time::Duration};

use crate::client::Client;
use crate::metadata_state::{MetadataPaths, MetadataStateStore};
use crate::publication::PublisherPolicy;
use crate::quic::QuicResourceSettings;
use crate::server::{
    InternalMetadata, Server, DEFAULT_MAX_REGISTERED_CLIENTS, HARD_MAX_REGISTERED_CLIENTS,
};

pub mod client;
pub mod error;
pub mod metadata_state;
pub mod publication;
pub mod quic;
pub mod rest_api;
pub mod server;

const DEFAULT_HTTP_BIND: &str = "127.0.0.1:3000";
const HTTP_BIND_ENV: &str = "DISTD_HTTP_BIND";
const ALLOW_INSECURE_REMOTE_HTTP_ENV: &str = "DISTD_ALLOW_INSECURE_REMOTE_HTTP";
const PUBLISHER_PUBLIC_KEYS_ENV: &str = "DISTD_PUBLISHER_PUBLIC_KEYS";
const ALLOW_UNSIGNED_PUBLICATION_ENV: &str = "DISTD_ALLOW_UNSIGNED_PUBLICATION";
const PUBLICATION_MEMORY_MIB_ENV: &str = "DISTD_PUBLICATION_MEMORY_MIB";
const DEFAULT_PUBLICATION_MEMORY_MIB: usize = 512;
const MAX_PUBLICATION_MEMORY_MIB: usize = 2_048;
const PUBLICATION_BODY_TIMEOUT_SECS_ENV: &str = "DISTD_PUBLICATION_BODY_TIMEOUT_SECS";
const DEFAULT_PUBLICATION_BODY_TIMEOUT_SECS: u64 = 600;
const MAX_PUBLICATION_BODY_TIMEOUT_SECS: u64 = 86_400;
const MAX_REGISTERED_CLIENTS_ENV: &str = "DISTD_MAX_REGISTERED_CLIENTS";
const MAX_PUBLISHER_KEYS: usize = 1_024;

#[derive(Clone)]
struct ConfiguredPublisherPolicy {
    policy: PublisherPolicy,
    fingerprints: Vec<String>,
}

fn decode_publisher_public_key(value: &str) -> Result<PublisherPublicKey, String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(
            "publisher keys must be exactly 64 lowercase hexadecimal characters".to_string(),
        );
    }
    let mut bytes = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let decode = |byte: u8| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err("publisher key contains invalid hexadecimal".to_string()),
        };
        bytes[index] = decode(pair[0])? << 4 | decode(pair[1])?;
    }
    Ok(PublisherPublicKey::from_bytes(bytes))
}

fn parse_publisher_policy(
    configured_keys: Option<&str>,
    configured_unsigned: Option<&str>,
) -> Result<ConfiguredPublisherPolicy, String> {
    let allow_unsigned = match configured_unsigned {
        None | Some("false") => false,
        Some("true") => true,
        Some(_) => {
            return Err(format!(
                "{ALLOW_UNSIGNED_PUBLICATION_ENV} must be exactly \"true\" or \"false\""
            ));
        }
    };

    if allow_unsigned {
        if configured_keys.is_some() {
            return Err(format!(
                "{ALLOW_UNSIGNED_PUBLICATION_ENV}=true cannot be combined with {PUBLISHER_PUBLIC_KEYS_ENV}"
            ));
        }
        return Ok(ConfiguredPublisherPolicy {
            policy: PublisherPolicy::explicitly_insecure_unsigned(),
            fingerprints: Vec::new(),
        });
    }

    let configured_keys = configured_keys.ok_or_else(|| {
        format!(
            "{PUBLISHER_PUBLIC_KEYS_ENV} must contain at least one publisher key unless {ALLOW_UNSIGNED_PUBLICATION_ENV}=true"
        )
    })?;
    let mut keys = Vec::new();
    let mut unique = HashSet::new();
    for (index, raw) in configured_keys.split(',').enumerate() {
        if index >= MAX_PUBLISHER_KEYS {
            return Err(format!(
                "{PUBLISHER_PUBLIC_KEYS_ENV} supports at most {MAX_PUBLISHER_KEYS} keys"
            ));
        }
        let value = raw.trim_matches(|character: char| character.is_ascii_whitespace());
        if value.is_empty() {
            return Err(format!(
                "{PUBLISHER_PUBLIC_KEYS_ENV} contains an empty entry"
            ));
        }
        let key = decode_publisher_public_key(value).map_err(|reason| {
            format!(
                "invalid {PUBLISHER_PUBLIC_KEYS_ENV} entry {}: {reason}",
                index + 1
            )
        })?;
        if !unique.insert(key) {
            return Err(format!(
                "{PUBLISHER_PUBLIC_KEYS_ENV} contains a duplicate key"
            ));
        }
        keys.push(key);
    }
    if keys.is_empty() {
        return Err(format!(
            "{PUBLISHER_PUBLIC_KEYS_ENV} must contain at least one publisher key"
        ));
    }
    let mut fingerprints = keys
        .iter()
        .map(|key| key.fingerprint().to_string())
        .collect::<Vec<_>>();
    fingerprints.sort_unstable();
    Ok(ConfiguredPublisherPolicy {
        policy: PublisherPolicy::required(keys),
        fingerprints,
    })
}

fn parse_publication_memory_mib(configured: Option<&str>) -> Result<usize, String> {
    let Some(configured) = configured else {
        return Ok(DEFAULT_PUBLICATION_MEMORY_MIB);
    };
    if configured.is_empty()
        || (configured.len() > 1 && configured.starts_with('0'))
        || !configured.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!(
            "{PUBLICATION_MEMORY_MIB_ENV} must be a canonical integer from 1 through {MAX_PUBLICATION_MEMORY_MIB}"
        ));
    }
    let mebibytes = configured.parse::<usize>().map_err(|_| {
        format!(
            "{PUBLICATION_MEMORY_MIB_ENV} must be a canonical integer from 1 through {MAX_PUBLICATION_MEMORY_MIB}"
        )
    })?;
    if !(1..=MAX_PUBLICATION_MEMORY_MIB).contains(&mebibytes) {
        return Err(format!(
            "{PUBLICATION_MEMORY_MIB_ENV} must be from 1 through {MAX_PUBLICATION_MEMORY_MIB}"
        ));
    }
    Ok(mebibytes)
}

fn publication_memory_bytes(mebibytes: usize) -> Result<usize, String> {
    let bytes = mebibytes
        .checked_mul(1024 * 1024)
        .ok_or_else(|| format!("{PUBLICATION_MEMORY_MIB_ENV} overflows the platform byte range"))?;
    if bytes > usize::MAX >> 3 {
        return Err(format!(
            "{PUBLICATION_MEMORY_MIB_ENV} exceeds the platform admission capacity"
        ));
    }
    Ok(bytes)
}

fn parse_publication_body_timeout_secs(configured: Option<&str>) -> Result<u64, String> {
    let Some(configured) = configured else {
        return Ok(DEFAULT_PUBLICATION_BODY_TIMEOUT_SECS);
    };
    if configured.is_empty()
        || (configured.len() > 1 && configured.starts_with('0'))
        || !configured.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!(
            "{PUBLICATION_BODY_TIMEOUT_SECS_ENV} must be a canonical integer from 1 through {MAX_PUBLICATION_BODY_TIMEOUT_SECS}"
        ));
    }
    let seconds = configured.parse::<u64>().map_err(|_| {
        format!(
            "{PUBLICATION_BODY_TIMEOUT_SECS_ENV} must be a canonical integer from 1 through {MAX_PUBLICATION_BODY_TIMEOUT_SECS}"
        )
    })?;
    if !(1..=MAX_PUBLICATION_BODY_TIMEOUT_SECS).contains(&seconds) {
        return Err(format!(
            "{PUBLICATION_BODY_TIMEOUT_SECS_ENV} must be from 1 through {MAX_PUBLICATION_BODY_TIMEOUT_SECS}"
        ));
    }
    Ok(seconds)
}

fn parse_max_registered_clients(configured: Option<&str>) -> Result<usize, String> {
    let Some(configured) = configured else {
        return Ok(DEFAULT_MAX_REGISTERED_CLIENTS);
    };
    if configured.is_empty()
        || (configured.len() > 1 && configured.starts_with('0'))
        || !configured.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!(
            "{MAX_REGISTERED_CLIENTS_ENV} must be a canonical integer from 1 through {HARD_MAX_REGISTERED_CLIENTS}"
        ));
    }
    let limit = configured.parse::<usize>().map_err(|_| {
        format!(
            "{MAX_REGISTERED_CLIENTS_ENV} must be a canonical integer from 1 through {HARD_MAX_REGISTERED_CLIENTS}"
        )
    })?;
    if !(1..=HARD_MAX_REGISTERED_CLIENTS).contains(&limit) {
        return Err(format!(
            "{MAX_REGISTERED_CLIENTS_ENV} must be from 1 through {HARD_MAX_REGISTERED_CLIENTS}"
        ));
    }
    Ok(limit)
}

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

fn load_metadata<T>(
    paths: MetadataPaths,
    storage: &T,
    publisher_policy: &PublisherPolicy,
) -> (std::sync::Arc<MetadataStateStore>, InternalMetadata)
where
    T: ChunkStorage,
{
    let (store, metadata) = MetadataStateStore::open_with_validator(paths, |metadata| {
        metadata.verify_storage_after_metadata_validation(storage)?;
        crate::metadata_state::validate_runtime_metadata_with_policy(metadata, publisher_policy)
    })
    .unwrap_or_else(|error| {
        eprintln!("Cannot load server metadata: {error}");
        std::process::exit(2);
    });
    storage
        .reconcile_published_revisions(&metadata.published_revisions())
        .unwrap_or_else(|error| {
            eprintln!("Cannot reconcile server revision storage: {error}");
            std::process::exit(2);
        });
    (store, metadata)
}

/// Run the server with a concrete storage backend.
async fn run_server<T>(
    server: Server<T>,
    http_addr: SocketAddr,
    quic_settings: QuicResourceSettings,
    publication_memory_bytes: usize,
    publication_body_timeout: Duration,
) where
    T: ChunkStorage + Send + Sync + std::fmt::Debug + 'static,
{
    let feed = Feed::new("A feed");
    let _ = server.expose_feed(feed).await;

    let app = rest_api::make_app(
        server.clone(),
        publication_memory_bytes,
        publication_body_timeout,
    );

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
    let max_registered_clients = optional_utf8_environment(MAX_REGISTERED_CLIENTS_ENV)
        .and_then(|value| parse_max_registered_clients(value.as_deref()))
        .unwrap_or_else(|error| {
            eprintln!("Invalid client registry configuration: {error}");
            std::process::exit(2);
        });
    tracing::info!(max_registered_clients, "configured client registry limit");
    let publisher_keys =
        optional_utf8_environment(PUBLISHER_PUBLIC_KEYS_ENV).unwrap_or_else(|error| {
            eprintln!("Invalid publisher configuration: {error}");
            std::process::exit(2);
        });
    let allow_unsigned =
        optional_utf8_environment(ALLOW_UNSIGNED_PUBLICATION_ENV).unwrap_or_else(|error| {
            eprintln!("Invalid publisher configuration: {error}");
            std::process::exit(2);
        });
    let publisher = parse_publisher_policy(publisher_keys.as_deref(), allow_unsigned.as_deref())
        .unwrap_or_else(|error| {
            eprintln!("Invalid publisher configuration: {error}");
            std::process::exit(2);
        });
    let publication_memory_mib = optional_utf8_environment(PUBLICATION_MEMORY_MIB_ENV)
        .and_then(|value| parse_publication_memory_mib(value.as_deref()))
        .unwrap_or_else(|error| {
            eprintln!("Invalid publication memory configuration: {error}");
            std::process::exit(2);
        });
    let publication_memory_bytes =
        publication_memory_bytes(publication_memory_mib).unwrap_or_else(|error| {
            eprintln!("Invalid publication memory configuration: {error}");
            std::process::exit(2);
        });
    let publication_body_timeout_secs =
        optional_utf8_environment(PUBLICATION_BODY_TIMEOUT_SECS_ENV)
            .and_then(|value| parse_publication_body_timeout_secs(value.as_deref()))
            .unwrap_or_else(|error| {
                eprintln!("Invalid publication body timeout configuration: {error}");
                std::process::exit(2);
            });
    let publication_body_timeout = Duration::from_secs(publication_body_timeout_secs);
    tracing::info!(
        publication_memory_mib,
        publication_body_timeout_secs,
        "configured aggregate REST publication-body budget"
    );
    if publisher.policy.permits_unsigned() {
        tracing::warn!(
            "publisher authentication is explicitly disabled; unsigned publication is insecure"
        );
    } else {
        tracing::info!(
            publisher_count = publisher.fingerprints.len(),
            publisher_fingerprints = ?publisher.fingerprints,
            "configured publisher trust anchors"
        );
    }
    let enrollment_token = required_secret("DISTD_ENROLLMENT_TOKEN");
    let state_dir = std::env::var("DISTD_SERVER_STATE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| distd_core::utils::settings::cache_dir().join("server-state"));
    let metadata_paths = MetadataPaths::in_state_directory(&state_dir);

    // Select storage backend via DISTD_STORAGE env var.
    // Supported values: "fs", "memory" (default when redb feature is off), "redb" (default).
    let storage_kind = std::env::var("DISTD_STORAGE").unwrap_or_else(|_| {
        if cfg!(feature = "redb") {
            "redb".to_string()
        } else {
            "fs".to_string()
        }
    });
    let publisher_policy = publisher.policy;

    match storage_kind.as_str() {
        "fs" => {
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
            let policy_for_load = publisher_policy.clone();
            let (storage, metadata_store, metadata) = tokio::task::spawn_blocking(move || {
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
                let (metadata_store, metadata) =
                    load_metadata(metadata_paths, &storage, &policy_for_load);
                (storage, metadata_store, metadata)
            })
            .await
            .unwrap_or_else(|error| {
                eprintln!("Server-state startup worker failed: {error}");
                std::process::exit(2);
            });
            let server = Server::new_ephemeral_with_metadata(storage, metadata.clone())
                .with_enrollment_token(enrollment_token.clone())
                .with_client_registry_limit(max_registered_clients)
                .with_publisher_policy(publisher_policy.clone())
                .with_metadata_store(metadata_store);
            run_server(
                server,
                http_addr,
                quic_settings,
                publication_memory_bytes,
                publication_body_timeout,
            )
            .await;
        }
        #[cfg(feature = "redb")]
        "redb" => {
            let db_path = distd_core::utils::settings::cache_dir().join("server.redb");
            tracing::info!("Using redb storage at {}", db_path.display());
            let policy_for_load = publisher_policy.clone();
            let (storage, metadata_store, metadata) = tokio::task::spawn_blocking(move || {
                let storage = distd_core::chunk_storage::redb::RedbStorage::new(&db_path)
                    .expect("Failed to open redb database");
                let (metadata_store, metadata) =
                    load_metadata(metadata_paths, &storage, &policy_for_load);
                (storage, metadata_store, metadata)
            })
            .await
            .unwrap_or_else(|error| {
                eprintln!("Server-state startup worker failed: {error}");
                std::process::exit(2);
            });
            let server = Server::new_ephemeral_with_metadata(storage, metadata.clone())
                .with_enrollment_token(enrollment_token.clone())
                .with_client_registry_limit(max_registered_clients)
                .with_publisher_policy(publisher_policy.clone())
                .with_metadata_store(metadata_store);
            run_server(
                server,
                http_addr,
                quic_settings,
                publication_memory_bytes,
                publication_body_timeout,
            )
            .await;
        }
        "memory" => {
            tracing::info!("Using in-memory storage (ephemeral)");
            let server = Server::new_ephemeral(HashMapStorage::default())
                .with_enrollment_token(enrollment_token.clone())
                .with_client_registry_limit(max_registered_clients)
                .with_publisher_policy(publisher_policy.clone());
            run_server(
                server,
                http_addr,
                quic_settings,
                publication_memory_bytes,
                publication_body_timeout,
            )
            .await;
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
        parse_http_bind_policy, parse_max_registered_clients, parse_publication_body_timeout_secs,
        parse_publication_memory_mib, parse_publisher_policy, publication_memory_bytes,
        ALLOW_INSECURE_REMOTE_HTTP_ENV, ALLOW_UNSIGNED_PUBLICATION_ENV, DEFAULT_HTTP_BIND,
        DEFAULT_PUBLICATION_BODY_TIMEOUT_SECS, DEFAULT_PUBLICATION_MEMORY_MIB, HTTP_BIND_ENV,
        MAX_PUBLICATION_BODY_TIMEOUT_SECS, MAX_PUBLICATION_MEMORY_MIB, MAX_PUBLISHER_KEYS,
        MAX_REGISTERED_CLIENTS_ENV, PUBLICATION_BODY_TIMEOUT_SECS_ENV, PUBLICATION_MEMORY_MIB_ENV,
        PUBLISHER_PUBLIC_KEYS_ENV,
    };
    use crate::server::{DEFAULT_MAX_REGISTERED_CLIENTS, HARD_MAX_REGISTERED_CLIENTS};

    fn publisher_key(index: usize) -> String {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

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

    #[test]
    fn publisher_policy_requires_exact_unique_lowercase_keys() {
        let first = publisher_key(1);
        let second = publisher_key(2);
        let configured = format!(" \t{first},\n{second} ");
        let parsed = parse_publisher_policy(Some(&configured), None).expect("secure policy");
        assert!(!parsed.policy.permits_unsigned());
        assert_eq!(parsed.fingerprints.len(), 2);

        let duplicate = format!("{first},{first}");
        let error = parse_publisher_policy(Some(&duplicate), None)
            .err()
            .expect("duplicate key");
        assert!(error.contains("duplicate"));
        assert!(!error.contains(&first));

        for invalid in [
            String::new(),
            "00".repeat(31),
            "00".repeat(33),
            "GG".repeat(32),
            "ab".repeat(32).to_uppercase(),
            format!("{first},"),
            format!("{first}, {second} extra"),
        ] {
            let error = parse_publisher_policy(Some(&invalid), None)
                .err()
                .expect("invalid key");
            assert!(error.contains(PUBLISHER_PUBLIC_KEYS_ENV));
            if !invalid.is_empty() {
                assert!(!error.contains(&invalid));
            }
        }
    }

    #[test]
    fn publisher_policy_is_fail_closed_unless_unsigned_mode_is_explicit() {
        let missing = parse_publisher_policy(None, None)
            .err()
            .expect("missing secure policy");
        assert!(missing.contains(PUBLISHER_PUBLIC_KEYS_ENV));
        assert!(missing.contains(ALLOW_UNSIGNED_PUBLICATION_ENV));

        let insecure = parse_publisher_policy(None, Some("true")).expect("explicit insecure mode");
        assert!(insecure.policy.permits_unsigned());
        assert!(insecure.fingerprints.is_empty());

        let key = publisher_key(7);
        assert!(parse_publisher_policy(Some(&key), Some("true")).is_err());
        for value in ["", "TRUE", "1", "yes", " true", "false "] {
            let error = parse_publisher_policy(None, Some(value))
                .err()
                .expect("invalid switch");
            assert_eq!(
                error,
                format!("{ALLOW_UNSIGNED_PUBLICATION_ENV} must be exactly \"true\" or \"false\"")
            );
            if !value.is_empty() {
                assert!(!error.contains(value));
            }
        }
    }

    #[test]
    fn publisher_policy_enforces_the_key_count_bound() {
        let exact = (0..MAX_PUBLISHER_KEYS)
            .map(publisher_key)
            .collect::<Vec<_>>()
            .join(",");
        let parsed = parse_publisher_policy(Some(&exact), None).expect("exact key limit");
        assert_eq!(parsed.fingerprints.len(), MAX_PUBLISHER_KEYS);

        let over = format!("{exact},{}", publisher_key(MAX_PUBLISHER_KEYS));
        let error = parse_publisher_policy(Some(&over), None)
            .err()
            .expect("over key limit");
        assert!(error.contains("at most"));
        assert!(!error.contains(&over));
    }

    #[test]
    fn publication_memory_budget_is_canonical_checked_and_bounded() {
        assert_eq!(
            parse_publication_memory_mib(None).unwrap(),
            DEFAULT_PUBLICATION_MEMORY_MIB
        );
        for value in [1usize, 512, MAX_PUBLICATION_MEMORY_MIB] {
            assert_eq!(
                parse_publication_memory_mib(Some(&value.to_string())).unwrap(),
                value
            );
            assert_eq!(
                publication_memory_bytes(value).unwrap(),
                value * 1024 * 1024
            );
        }
        for value in [
            "",
            "0",
            "01",
            "+1",
            "-1",
            "1.0",
            " 1",
            "2049",
            "18446744073709551616",
            "not-a-number",
        ] {
            let error = parse_publication_memory_mib(Some(value)).unwrap_err();
            assert!(error.contains(PUBLICATION_MEMORY_MIB_ENV));
        }
    }

    #[test]
    fn publication_body_timeout_is_canonical_checked_and_bounded() {
        assert_eq!(
            parse_publication_body_timeout_secs(None).unwrap(),
            DEFAULT_PUBLICATION_BODY_TIMEOUT_SECS
        );
        for value in [1u64, 600, MAX_PUBLICATION_BODY_TIMEOUT_SECS] {
            assert_eq!(
                parse_publication_body_timeout_secs(Some(&value.to_string())).unwrap(),
                value
            );
        }
        for value in [
            "",
            "0",
            "01",
            "+1",
            "-1",
            "1.0",
            " 1",
            "86401",
            "18446744073709551616",
            "not-a-number",
        ] {
            let error = parse_publication_body_timeout_secs(Some(value)).unwrap_err();
            assert!(error.contains(PUBLICATION_BODY_TIMEOUT_SECS_ENV));
        }
    }

    #[test]
    fn registered_client_limit_is_canonical_checked_and_bounded() {
        assert_eq!(
            parse_max_registered_clients(None).unwrap(),
            DEFAULT_MAX_REGISTERED_CLIENTS
        );
        for value in [1usize, 4_096, HARD_MAX_REGISTERED_CLIENTS] {
            assert_eq!(
                parse_max_registered_clients(Some(&value.to_string())).unwrap(),
                value
            );
        }
        for value in [
            "",
            "0",
            "01",
            "+1",
            "-1",
            "1.0",
            " 1",
            "65537",
            "18446744073709551616",
            "not-a-number",
        ] {
            let error = parse_max_registered_clients(Some(value)).unwrap_err();
            assert!(error.contains(MAX_REGISTERED_CLIENTS_ENV));
        }
    }
}
