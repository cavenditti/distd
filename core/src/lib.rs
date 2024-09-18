#![feature(allocator_api)]
#![feature(map_try_insert)]
#![feature(array_chunks)]
#![feature(slice_as_chunks)]
#![feature(slice_pattern)]
#![feature(extract_if)]
#![feature(hash_extract_if)]
#![feature(test)]

pub mod chunk_storage;
pub mod chunks;
pub mod error;
pub mod feed;
pub mod hash;
pub mod item;
pub mod metadata;
pub mod peer;
pub mod unique_name;
pub mod utils;
pub mod version;

pub mod benchmarks;

pub mod proto {
    tonic::include_proto!("distd");
}

pub use tonic;

pub type Client<T> = proto::distd_client::DistdClient<T>;
pub type Server<T> = proto::distd_server::DistdServer<T>;

pub type Request<T> = tonic::Request<T>;

pub type GrpcError = tonic::Status;
pub type TransportError = tonic::transport::Error;
