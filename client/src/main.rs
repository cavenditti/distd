//#![deny(warnings)]
#![feature(iter_advance_by)]
#![feature(async_closure)]
#![warn(rust_2018_idioms)]

pub mod client;
pub mod error;
pub mod server;
pub mod settings;

pub use error::Client as ClientError;

#[tokio::main]
pub async fn main() -> Result<(), ClientError> {
    client::cli::main().await
}
