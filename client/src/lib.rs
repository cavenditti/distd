//! Authenticated distd client APIs, also used by the command-line client.
#![warn(rust_2018_idioms)]

pub mod client;
pub mod error;
pub mod persistence;
pub mod publisher;
pub mod rollback;
pub mod server;
pub mod server_url;
pub mod session;
pub mod settings;

pub use error::Client as ClientError;
