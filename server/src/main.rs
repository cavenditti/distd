use distd_core::chunk_storage::fs_storage::{FsStorage, FsStorageCacheConfig};
use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::feed::Feed;

use crate::client::Client;
use crate::server::Server;

pub mod client;
pub mod error;
pub mod rest_api;
pub mod quic;
pub mod server;

/// Run the server with a concrete storage backend.
async fn run_server<T>(server: Server<T>)
where
    T: ChunkStorage + Send + Sync + std::fmt::Debug + 'static,
{
    let feed = Feed::new("A feed");
    server.expose_feed(feed).await.unwrap();

    let app = rest_api::make_app(server.clone());

    let addr_quic = "0.0.0.0:50051".parse().unwrap();
    tokio::spawn(async move {
        if let Err(err) = server.clone().serve_quic(addr_quic).await {
            tracing::error!("QUIC server failed: {err}");
        }
    });
    tracing::info!("listening on {} for QUIC", addr_quic);

    // run our app with hyper, listening globally on port 3000
    let addr = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::info!("listening on {} for HTTP", addr);

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
            let server = Server::new_ephemeral(FsStorage::with_cache_config(
                root,
                FsStorageCacheConfig {
                    max_chunk_bytes: chunk_cache_mb.saturating_mul(1024 * 1024),
                    max_tree_entries: tree_cache_entries,
                },
            ));
            run_server(server).await;
        }
        #[cfg(feature = "redb")]
        "redb" => {
            let db_path = distd_core::utils::settings::cache_dir().join("server.redb");
            tracing::info!("Using redb storage at {}", db_path.display());
            let storage = distd_core::chunk_storage::redb::RedbStorage::new(&db_path)
                .expect("Failed to open redb database");
            let server = Server::new_ephemeral(storage);
            run_server(server).await;
        }
        "memory" => {
            tracing::info!("Using in-memory storage (ephemeral)");
            let server = Server::new_ephemeral(HashMapStorage::default());
            run_server(server).await;
        }
        other => {
            eprintln!("Unknown DISTD_STORAGE value: {other}. Supported: \"fs\", \"redb\", \"memory\".");
            std::process::exit(1);
        }
    }
}
