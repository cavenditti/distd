use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
use distd_core::feed::Feed;

use crate::client::Client;
use crate::server::Server;

pub mod client;
pub mod error;
pub mod rest_api;
pub mod grpc;
pub mod server;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    // TODO: load key from disk; for now generate an ephemeral key (data is lost on restart)
    let server: Server<HashMapStorage> = Server::new_ephemeral(HashMapStorage::default());
    let feed = Feed::new("A feed");
    server.expose_feed(feed).await.unwrap();

    let app = rest_api::make_app(server.clone());

    let addr_grpc = "[::1]:50051".parse().unwrap();
    let grpc_service = server.clone().make_grpc_service().await.unwrap();
    tokio::spawn(grpc_service.serve(addr_grpc));
    tracing::info!("listening on {} for gRPC", addr_grpc);

    // run our app with hyper, listening globally on port 3000
    let addr = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::info!("listening on {} for HTTP", addr);

    axum::serve(listener, app).await.unwrap();
}
