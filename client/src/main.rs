//#![deny(warnings)]
#![feature(iter_advance_by)]
#![warn(rust_2018_idioms)]
use std::{env, fmt::Write, path::PathBuf, str::FromStr, thread::sleep, time::Duration};

use config::Config;

use distd_core::chunk_storage::fs_storage::FsStorage;
use http_body_util::BodyExt;
use hyper::body::Buf;

use crate::client::Client;

pub mod client;
pub mod connection;
pub mod server;

#[tokio::main]
async fn main() -> Result<(), i32> {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    let cmd = std::env::args().nth(1).expect("no cmd given");
    let mut i = std::env::args();
    i.advance_by(2).expect("Invalid arguments");
    let cmd_args = i.collect::<Vec<String>>();
    println!("CMD: {} {:?}", cmd, cmd_args);

    let settings = Config::builder()
        // Add in `./Settings.toml`
        .add_source(config::File::with_name("ClientSettings"))
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
        .add_source(config::Environment::with_prefix("DISTD"))
        .build()
        .expect("Missing configuration file");

    println!("Config: {:?}", settings);

    let url = settings
        .get_string("server_url")
        .expect("Missing server url in configuration");
    let uri = url.parse::<hyper::Uri>().unwrap();

    let storage = FsStorage::new(PathBuf::from_str("here").unwrap()).unwrap();
    let client = loop {
        match client::Client::new(uri.clone(), &[0u8; 32], storage.clone()).await {
            Ok(client) => break client,
            Err(e) => {
                const T: u64 = 5;
                println!("Error: '{}', retrying in {} seconds", e, T);
                sleep(Duration::from_secs(T))
            }
        }
    };

    println!("Feeds: {:?}", client.server.metadata().await.feeds);
    println!("Items: {:?}", client.server.metadata().await.items);

    match cmd.as_str() {
        "fetch" => fetch(client, cmd_args).await,
        "loop" => client_loop(client).await,
        _ => {
            println!("Invalid command specified");
            return Err(-3);
        }
    }
}

async fn client_loop(client: Client<FsStorage>) -> Result<(), i32> {
    client.server.fetch_loop().await;
    Ok(())
}

async fn fetch(client: Client<FsStorage>, args: Vec<String>) -> Result<(), i32> {
    let (url, method) = args.get(1).zip(args.get(2)).expect("Invalid args");
    println!("Fetch {} {}", method, url);

    // HTTPS requires picking a TLS implementation, so give a better
    // warning if the user tries to request an 'https' URL.
    let url = url.parse::<hyper::Uri>().unwrap();
    if url.scheme_str() != Some("http") {
        println!("This example only works with 'http' URLs.");
        return Ok(());
    }

    let mut response = client
        .server
        .send_request(method, url.path_and_query().unwrap().clone())
        .await
        .inspect(|r| println!("Got {:?}", &r))
        .inspect_err(|e| println!("{}", e))
        .map_err(|_| -7)?;

    // send help plz
    let body = response
        .body_mut()
        .collect()
        .await
        .unwrap()
        .aggregate()
        .chunk()
        .to_vec();
    let body_str =
        String::from_utf8(body.clone()).unwrap_or(body.iter().fold(String::new(), |mut s, x| {
            let _ = write!(s, "{x:x?}");
            s
        }));
    //.unwrap_or(body.iter().map(|x| format!("{:x?}", x)).collect());
    println!("Body: `{}`", body_str);
    Ok(())
}
