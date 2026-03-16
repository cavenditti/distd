//#![deny(warnings)]
#![warn(rust_2018_idioms)]

pub mod client;
pub mod error;
pub mod persistence;
pub mod server;
pub mod settings;

pub use error::Client as ClientError;

#[tokio::main]
pub async fn main() -> Result<(), ClientError> {
    client::cli::main().await
}
