use std::sync::Arc;

use blake3::Hash;

use crate::metadata::ChunkInfo;

pub mod hashmap_storage;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Unknown storage size")]
    UnknownSize,
}

/// Defines a backend used to store hashes and chunks ad key-value pairs
pub trait ChunkStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<Vec<u8>>>;
    fn insert(&self, chunk: &[u8]) -> Option<ChunkInfo>;
    fn chunks(&self) -> Vec<Hash>;
    /// Allocated size for all chunks, in bytes
    /// This only counts actual chunks size, excluding any auxiliary structure used by storage backend/adapter
    fn size(&self) -> usize;
    //fn drop(hash: Hash); // ??
}
