use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use blake3::Hash;

use crate::chunk_storage::ChunkStorage;
use crate::hash::merge_hashes;

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
            println!("[StorageInsert] Hash: {}, size: {}", hash, size);
            if let Some(raw_chunk) = data.get(&hash.clone()) {
                return Some(raw_chunk.clone());
            }
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
        println!("[Storage Link ]: {}: {} + {}", hash, left.get_hash(), right.get_hash());
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


#[cfg(test)]
mod tests {
    use crate::metadata::CHUNK_SIZE;

    use super::*;
    use bytes::Bytes;
    use ptree::print_tree;

    #[test]
    fn test_hms_single_chunk_insertion() {
        let s = HashMapStorage::default();
        let data = Bytes::from_static(b"very few bytes");
        let len = data.len();
        s.insert(data);
        assert_eq!(len, s.size());
    }

    #[test]
    /// Multiple chunks, not aligned with CHUNK_SIZE
    fn test_hms_insertion() {
        let s = HashMapStorage::default();
        let data = Bytes::from_static(include_bytes!("../../../Cargo.lock"));
        let len = data.len();
        let root = s.insert(data).unwrap();
        println!("\nOriginal lenght: {}, stored length: {}", len, s.size());
        print_tree(&*root.to_owned()).unwrap();
        println!();
        assert!(len > s.size());
    }

    //fn test_hms_multi_chunk_insertion() {

    #[test]
    fn test_hms_deduplicated_insertion() {
        let s = HashMapStorage::default();
        let data = Bytes::from_static(&[0u8; CHUNK_SIZE * 3]);
        println!("{:?}", data.len());
        let root = s.insert(data).unwrap();
        print_tree(&*root.to_owned()).unwrap();
        assert_eq!(CHUNK_SIZE, s.size());
    }

    #[test]
    fn test_hms_2mb_insertion() {
        let s = HashMapStorage::default();
        let data = Bytes::from_static(include_bytes!("../../../1.MOV"));
        let len = data.len();
        s.insert(data);
        assert_eq!(len, s.size());
    }
}

