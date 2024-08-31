//#![deny(warnings)]
#![warn(rust_2018_idioms)]
use std::{env, path::PathBuf, str::FromStr, thread::sleep, time::Duration};

use distd_core::chunk_storage::fs_storage::FsStorage;

pub mod client;
pub mod connection;
pub mod server;

// A simple type alias so as to DRY.
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Result<()> {
    // Some simple CLI args requirements...
    let method = match env::args().nth(1) {
        Some(method) => method,
        None => {
            println!("Usage: client <HTTP method> <url>");
            return Ok(());
        }
    };
    let url = match env::args().nth(2) {
        Some(url) => url,
        None => {
            println!("Usage: client <url>");
            return Ok(());
        }
    };

    // HTTPS requires picking a TLS implementation, so give a better
    // warning if the user tries to request an 'https' URL.
    let url = url.parse::<hyper::Uri>().unwrap();
    if url.scheme_str() != Some("http") {
        println!("This example only works with 'http' URLs.");
        return Ok(());
    }

    fetch_url(url, method).await
}

async fn fetch_url(url: hyper::Uri, method: String) -> Result<()> {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    let storage = FsStorage::new(PathBuf::from_str("here").unwrap()).unwrap();
    let client = loop {
        match client::Client::new(url.clone(), &[0u8; 32], storage.clone()).await {
            Ok(client) => break client,
            Err(e) => {
                const T: u64 = 5;
                println!("Error: '{}', retrying in {} seconds", e, T);
                sleep(Duration::from_secs(T))
            }
        }
    };
    client
        .server
        .send_request(&method, url.path_and_query().unwrap().clone())
        .await
        .unwrap(); //FIXME remove this

    client.server.fetch_loop().await;

    Ok(())
}
