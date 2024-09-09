//#![deny(warnings)]
#![feature(iter_advance_by)]
#![warn(rust_2018_idioms)]

pub mod client;
pub mod connection;
pub mod error;
pub mod server;
pub mod settings;

use error::ClientError;

#[tokio::main]
pub async fn main() -> Result<(), ClientError> {
    client::cli::main().await
}
