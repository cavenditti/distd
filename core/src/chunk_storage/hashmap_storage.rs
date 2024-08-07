use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use blake3::Hash;

use crate::chunk_storage::ChunkStorage;
use crate::metadata::{ChunkInfo, RawChunk};

// Dead simple in-memory global storage
#[derive(Default, Clone)]
pub struct HashMapStorage {
    // This re-hashes the hashes, but nicely handles collisions in return
    // TODO we may use a Hasher that just returns the first n bytes of the SHA-256?
    data: Arc<RwLock<HashMap<Hash, RawChunk>>>,
}

impl ChunkStorage for HashMapStorage {
    fn get(&self, hash: &Hash) -> Option<RawChunk> {
        self.data.read().unwrap().get(hash).cloned()
    }

    fn insert(&self, chunk: &[u8]) -> Option<ChunkInfo> {
        if let Ok(mut data) = self.data.write() {
            let size: u32 = chunk.len().try_into().unwrap(); // FIXME unwrap
            let hash = blake3::hash(&chunk);
            if let Some(_raw_chunk) = data.get(&hash.clone()) {
                return Some(ChunkInfo { size, hash });
            }
            let chunk_info = ChunkInfo { size, hash };
            data.insert(hash, Arc::new(Vec::from(chunk)))
                .and(Some(chunk_info))
        } else {
            None
        }
    }

    fn chunks(&self) -> Vec<Hash> {
        self.data.read().unwrap().keys().cloned().collect()
    }

    fn size(&self) -> usize {
        self.data.read().unwrap().values().map(|x| x.len()).sum()
    }
}
