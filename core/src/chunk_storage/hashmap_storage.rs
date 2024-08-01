use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use blake3::Hash;

use crate::metadata::RawChunk;
use crate::chunk_storage::ChunkStorage;

// Dead simple in-memory global storage
#[derive(Default, Clone)]
pub struct HashMapStorage {
    // This re-hashes the hashes, but nicely handles collisions in return
    // TODO we may use a Hasher that just returns the first n bytes of the SHA-256?
    data: Arc<RwLock<HashMap<Hash, RawChunk>>,>
}

impl ChunkStorage for HashMapStorage {
    fn get(self, hash: &Hash) -> Option<RawChunk> {
        let data = match self.data.read() {
            Ok(data) => data,
            Err(_) => return None,
        };
        data.get(hash).copied()
    }

    fn insert(self, chunk: RawChunk) -> Option<Hash> {
        let mut data = match self.data.write() {
            Ok(data) => data,
            Err(_) => return None,
        };
        let hash = blake3::hash(&chunk);
        match data.insert(hash, chunk) {
            Some(_) => Some(hash),
            None => None,
        }
    }
}
