#![feature(allocator_api)]
#![feature(map_try_insert)]
#![feature(array_chunks)]
#![feature(slice_as_chunks)]
#![feature(slice_pattern)]
#![feature(test)]


pub mod version;
pub mod hash;
pub mod metadata;
pub mod chunk_storage;
pub mod unique_name;
pub mod feed;
pub mod net;
pub mod msgpack;
pub mod peer;
pub mod utils;

pub mod benchmarks;
