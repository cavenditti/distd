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
    data: Arc<RwLock<HashMap<Hash, StoredChunkRef>>>,
}

impl ChunkStorage for HashMapStorage {
    fn get(&self, hash: &Hash) -> Option<StoredChunkRef> {
        self.data.read().unwrap().get(hash).cloned()
    }

    fn insert(&self, chunk: &[u8]) -> Option<ChunkInfo> {
        if let Ok(mut data) = self.data.write() {
            //println!("Chunk: {:?}", &chunk);
            let size: u32 = chunk.len().try_into().unwrap(); // FIXME unwrap
            let hash = blake3::hash(&chunk);
            println!("Hash: {}, size: {}", hash, size);
            if let Some(_raw_chunk) = data.get(&hash.clone()) {
                return Some(ChunkInfo { size, hash });
            }
            let chunk_info = ChunkInfo { size, hash };
            data.try_insert(hash, StoredChunkRef::Stored(Arc::new(Vec::from(chunk))))
                .ok()
                .map(|_| chunk_info)
        } else {
            None
        }
    }

    fn link(&self, children: (&ChunkInfo, &ChunkInfo)) -> Option<ChunkInfo> {
        let hash = merge_hashes(&children.0.hash, &children.1.hash);
        if let Ok(mut data) = self.data.write() {
            data.try_insert(
                hash,
                StoredChunkRef::Parent {
                    left: children.0.hash,
                    right: children.1.hash,
                },
            )
            .ok()
            .map(|_| ChunkInfo {
                size: children.0.size + children.1.size,
                hash,
            })
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
            .map(|x| match x {
                StoredChunkRef::Stored(x) => x.len(),
                _ => 0,
            })
            .sum()
    }
}
