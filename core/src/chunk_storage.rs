use blake3::Hash;

use crate::metadata::RawChunk;
pub mod hashmap_storage;

/// Defines a backend used to store hashes and chunks ad key-value pairs
pub trait ChunkStorage {
    fn get(self, hash: &Hash) -> Option<RawChunk>;
    fn insert(self, chunk: RawChunk) -> Option<Hash>;
    //fn drop(hash: Hash); // ??
}
