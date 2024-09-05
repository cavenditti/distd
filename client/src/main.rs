//#![deny(warnings)]
#![feature(iter_advance_by)]
#![warn(rust_2018_idioms)]
use std::{
    env, fmt::Write, fs::File, io::Read, path::PathBuf, str::FromStr, thread::sleep, time::Duration,
};

use config::Config;

use error::ClientError;
use http_body_util::BodyExt;
use hyper::body::Buf;
use hyper::http::uri::PathAndQuery;

use distd_core::{
    chunk_storage::{fs_storage::FsStorage, ChunkStorage},
    chunks::flatten,
};

use crate::client::Client;

pub mod client;
pub mod connection;
pub mod error;
pub mod server;

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .with_max_level(tracing::Level::TRACE)
        .init();

    tracing::info!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    let cmd = std::env::args().nth(1).ok_or(ClientError::InvalidArgs)?;
    let mut i = std::env::args();
    i.advance_by(2).map_err(|_| ClientError::InvalidArgs)?;
    let cmd_args = i.collect::<Vec<String>>();
    tracing::debug!("Running \"{cmd}\" {cmd_args:?}");

    let settings = Config::builder()
        // Add in `./ClientSettings.toml`
        .add_source(config::File::with_name("ClientSettings"))
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `DISTD_DEBUG=1 ./target/app` would set the `debug` key
        .add_source(config::Environment::with_prefix("DISTD"))
        .build()
        .expect("Missing configuration file");

    tracing::debug!("Config: {settings:?}");

    let url = settings
        .get_string("server_url")
        .expect("Missing server url in configuration");
    let uri = url.parse::<hyper::Uri>().unwrap();

    // Only HTTP for now.
    if uri.scheme_str() != Some("http") {
        println!("This example only works with 'http' URLs.");
        return Err(ClientError::InvalidArgs);
    }

    let storage = FsStorage::new(PathBuf::from_str("here").unwrap()).unwrap();
    let client = loop {
        match client::Client::new("Some name", uri.clone(), &[0u8; 32], storage.clone()).await {
            Ok(client) => break client,
            Err(e) => {
                const T: u64 = 5;
                println!("Error: '{e}', retrying in {T} seconds");
                sleep(Duration::from_secs(T));
            }
        }
    };

    match cmd.as_str() {
        "fetch" => fetch(client, cmd_args).await,
        "loop" => client_loop(client).await,
        "sync" => sync(client, &cmd_args[..]).await,
        _ => {
            println!("Invalid command specified");
            Err(ClientError::InvalidArgs)
        }
    }
}

async fn sync(client: Client<FsStorage>, args: &[String]) -> Result<(), ClientError> {
    let (target, path) = match args.len() {
        1 => Ok((args.get(0).unwrap(), args.get(0).unwrap())),
        2 => Ok((args.get(0).unwrap(), args.get(1).unwrap())),
        _ => Err(ClientError::InvalidArgs),
    }?;
    let path = PathBuf::from_str(path).unwrap();

    let mut buf = vec![];

    let from = client.storage.chunks(); // FIXME this could get very very large

    if path.exists() {
        let mut f = File::open(&path).expect("Invalid or unreachable file path");
        f.read_to_end(&mut buf).unwrap();
    }

    let server_metadata = client.server.metadata().await;
    let item = server_metadata
        .items
        .get(&PathBuf::from_str(target.as_str()).unwrap())
        .ok_or(ClientError::FileNotFound(target.to_string()))?;

    let insertion_result = client.storage.create_item(
        item.name.clone(),
        path.clone(),
        item.revision,
        item.description.clone(),
        buf.clone().into(),
    );
    println!("{:?}", insertion_result);

    let result = client
        .server
        .transfer_diff(&item.root.hash.to_string(), from)
        .await
        .inspect_err(|e| println!("Error on transfer_diff: {e}"))
        .unwrap();

    tracing::trace!("Got {} chunks from server", result.len());
    let flat = flatten(result)?;
    tracing::trace!("{} bytes total", flat.len());

    let _insertion_result = client
        .storage
        .create_item(
            item.name.clone(),
            path.into(),
            item.revision,
            item.description.clone(),
            flat.into(),
        )
        .ok_or(ClientError::IoError("Cannot insert downloaded file".into()))?;
    Ok(())
}

/// Main client loop
async fn client_loop(client: Client<FsStorage>) -> Result<(), ClientError> {
    client.server.fetch_loop().await;
    Ok(())
}

/// Fetch a resource from the server, mostly used for debug
async fn fetch(client: Client<FsStorage>, args: Vec<String>) -> Result<(), ClientError> {
    let (method, url) = args.get(0).zip(args.get(1)).expect("Invalid args");
    tracing::debug!("Fetch {method} {url}");

    // url should always start with exactly one "/"
    let url = format!("/{}", url.trim_start_matches("/"));

    let mut response = client
        .server
        .send_request(
            method,
            PathAndQuery::from_str(url.as_str()).expect("Invalid path specified"),
        )
        .await
        .inspect(|r| tracing::debug!("Got {:?}", &r))
        .inspect_err(|e| println!("{e}"))
        .map_err(|_| ClientError::ServerRequestError)?;

    let body = response
        .body_mut()
        .collect()
        .await
        .map_err(|_| ClientError::ServerResponseError)?
        .aggregate()
        .chunk()
        .to_vec();

    let body_str =
        String::from_utf8(body.clone()).unwrap_or(body.iter().fold(String::new(), |mut s, x| {
            let _ = write!(s, "{x:x?}");
            s
        }));
    tracing::trace!("Body: `{body_str}`");
    Ok(())
}
