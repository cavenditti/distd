#![feature(map_try_insert)]

use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::feed::{Feed, FeedName};

use crate::client::Client;
use crate::server::Server;

pub mod client;
pub mod server;
pub mod rest_api;
pub mod error;

#[tokio::main]
async fn main() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    let server: Server<HashMapStorage> = Server::default();
    let feed = Feed::new("A feed");
    server.expose_feed(feed);

    let app = rest_api::make_app(server);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
