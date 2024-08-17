use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use blake3::Hash;

use crate::chunk_storage::ChunkStorage;
use crate::hash::merge_hashes;
use crate::metadata::ChunkInfo;

use super::StoredChunkRef;

// Dead simple in-memory global storage
#[derive(Default, Clone)]
pub struct HashMapStorage {
    // This re-hashes the hashes, but nicely handles collisions in return
    // TODO we may use a Hasher that just returns the first n bytes of the SHA-256?
    data: Arc<RwLock<HashMap<Hash, Arc<StoredChunkRef>>>>,
}

impl ChunkStorage for HashMapStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<StoredChunkRef>> {
        self.data.read().unwrap().get(hash).cloned()
    }

    fn insert_chunk(&self, chunk: &[u8]) -> Option<Arc<StoredChunkRef>> {
        if let Ok(mut data) = self.data.write() {
            //println!("Chunk: {:?}", &chunk);
            let size: u32 = chunk.len().try_into().unwrap(); // FIXME unwrap
            let hash = blake3::hash(&chunk);
            println!("Hash: {}, size: {}", hash, size);
            if let Some(raw_chunk) = data.get(&hash.clone()) {
                return Some(raw_chunk.clone());
            }
            let chunk_info = ChunkInfo { size, hash };
            data.try_insert(
                hash,
                Arc::new(StoredChunkRef::Stored {
                    hash,
                    data: Arc::new(Vec::from(chunk)),
                }),
            )
            .ok()
            .map(|x| x.clone())
        } else {
            None
        }
    }

    fn link(
        &self,
        left: Arc<StoredChunkRef>,
        right: Arc<StoredChunkRef>,
    ) -> Option<Arc<StoredChunkRef>> {
        let hash = merge_hashes(left.get_hash(), right.get_hash());
        if let Ok(mut data) = self.data.write() {
            data.try_insert(hash, Arc::new(StoredChunkRef::Parent { hash, left, right }))
                .ok()
                .map(|x| x.clone())
        } else {
            None
        }
    }

    fn chunks(&self) -> Vec<Hash> {
        self.data.read().unwrap().keys().cloned().collect()
    }

    fn size(&self) -> usize {
        self.data
            .read()
            .unwrap()
            .values()
            .map(|x| match &**x {
                StoredChunkRef::Stored { hash: _, data } => data.len(),
                _ => 0,
            })
            .sum()
    }
}
