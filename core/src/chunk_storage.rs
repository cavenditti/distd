use blake3::Hash;

use crate::metadata::{ChunkInfo, RawChunk};

pub mod hashmap_storage;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Unknown storage size")]
    UnknownSize,
    #[error("Cannot insert chunk in data store")]
    UnknownChunkInsertError(#[from] std::io::Error),
}

#[derive(Clone)]
pub enum StoredChunkRef {
    Parent { left: Hash, right: Hash },
    Stored(RawChunk),
}

/// Defines a backend used to store hashes and chunks ad key-value pairs
pub trait ChunkStorage {
    fn get(&self, hash: &Hash) -> Option<StoredChunkRef>;
    fn insert(&self, chunk: &[u8]) -> Option<ChunkInfo>;
    fn link(&self, children: (&ChunkInfo, &ChunkInfo)) -> Option<ChunkInfo>;
    fn chunks(&self) -> Vec<Hash>;
    /// Allocated size for all chunks, in bytes
    /// This only counts actual chunks size, excluding any auxiliary structure used by storage backend/adapter
    fn size(&self) -> usize;
    //fn drop(hash: Hash); // ??
}
