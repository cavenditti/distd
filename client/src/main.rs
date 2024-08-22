//#![deny(warnings)]
#![warn(rust_2018_idioms)]
use std::env;

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::Request;
use tokio::io::{self, AsyncWriteExt as _};

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
    println!("{}", method);
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
    let client = client::Client::new(url.clone(), &[0u8; 32]).await;
    println!("{:?}", client.server.get_metadata().await);

    let path = url.path();
    let mut res = client.server.send_request(&method, path).await.unwrap();

    println!("Response: {}", res.status());
    println!("Headers: {:#?}\n", res.headers());

    // Stream the body, writing each chunk to stdout as we get it
    // (instead of buffering and printing at the end).
    while let Some(next) = res.frame().await {
        let frame = next?;
        if let Some(chunk) = frame.data_ref() {
            io::stdout().write_all(chunk).await?;
        }
    }

    println!("\n\nDone!");


    /*
    tokio::task::spawn(async move {
        client.server.fetch_loop().await;
    });
    */
    client.server.fetch_loop().await;

    Ok(())
}
