// `extract_if`/`hash_extract_if` stabilised in 1.87/1.88; `map_try_insert` still unstable.
// `test` is nightly-only; keep it only for nightly builds where benchmarks are run.
#![cfg_attr(all(test, feature = "nightly-bench"), feature(test))]

pub mod chunk_storage;
pub mod chunks;
pub mod error;
pub mod feed;
pub mod hash;
pub mod item;
pub mod metadata;
pub mod possession;
pub mod unique_name;
pub mod utils;
pub mod version;

#[allow(clippy::all)]
#[allow(warnings)]
pub mod proto {
    tonic::include_proto!("distd");
}

pub use tonic;

pub type Client<T> = proto::distd_client::DistdClient<T>;
pub type Server<T> = proto::distd_server::DistdServer<T>;

pub type Request<T> = tonic::Request<T>;

pub type GrpcError = tonic::Status;
pub type TransportError = tonic::transport::Error;
