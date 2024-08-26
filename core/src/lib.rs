#![feature(allocator_api)]
#![feature(map_try_insert)]
#![feature(array_chunks)]
#![feature(slice_as_chunks)]
#![feature(slice_pattern)]
#![feature(extract_if)]
#![feature(hash_extract_if)]
#![feature(test)]


pub mod version;
pub mod hash;
pub mod chunks;
pub mod chunk_storage;
pub mod unique_name;
pub mod feed;
pub mod peer;
pub mod utils;
pub mod metadata;
pub mod item;

pub mod benchmarks;
