#![feature(map_try_insert)]

use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::feed::{Feed, FeedName};

use crate::client::Client;
use crate::server::Server;

pub mod client;
pub mod error;
pub mod rest_api;
pub mod server;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    tracing::info!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    let server: Server<HashMapStorage> = Server::default();
    let feed = Feed::new("A feed");
    server.expose_feed(feed).unwrap();

    let app = rest_api::make_app(server);

    // run our app with hyper, listening globally on port 3000
    let addr = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    tracing::info!("listening on {}", addr);

    axum::serve(listener, app).await.unwrap();
}
