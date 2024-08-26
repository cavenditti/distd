use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use blake3::Hash;

use crate::chunk_storage::ChunkStorage;

use super::StoredChunkRef;

/// Dead simple in-memory global storage
#[derive(Default, Clone)]
pub struct HashMapStorage {
    data: Arc<RwLock<HashMap<Hash, Arc<StoredChunkRef>>>>,
}

impl ChunkStorage for HashMapStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<StoredChunkRef>> {
        self.data.read().unwrap().get(hash).cloned()
    }

    fn _insert_chunk(&self, hash: Hash, chunk: &[u8]) -> Option<Arc<StoredChunkRef>> {
        if let Ok(mut data) = self.data.write() {
            //println!("[StorageInsert] Hash: {}, size: {}", hash, size);
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

    fn _link(
        &self,
        hash: Hash,
        left: Arc<StoredChunkRef>,
        right: Arc<StoredChunkRef>,
    ) -> Option<Arc<StoredChunkRef>> {
        /*
        println!(
            "[Storage Link ]: {}: {} + {}",
            hash,
            left.get_hash(),
            right.get_hash()
        );
        */
        if let Ok(mut data) = self.data.write() {
            data.get(&hash).cloned().or(data
                .try_insert(hash, Arc::new(StoredChunkRef::Parent { hash, left, right }))
                .ok()
                .map(|x| x.clone()))
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
                StoredChunkRef::Stored { data, .. } => data.len(),
                _ => 0,
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use crate::{chunks::CHUNK_SIZE, hash::hash};

    use super::*;
    use bytes::Bytes;
    //use ptree::print_tree;

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
        //let root = s.insert(data).unwrap();
        println!("\nOriginal lenght: {}, stored length: {}", len, s.size());
        //print_tree(&*root.to_owned()).unwrap();
        println!();
        assert!(len >= s.size());
    }

    //fn test_hms_multi_chunk_insertion() {

    #[test]
    fn test_hms_deduplicated() {
        const SIZE: usize = CHUNK_SIZE * 3;
        let s = HashMapStorage::default();
        let data = Bytes::from_static(&[0u8; SIZE]);
        println!("{:?}", data.len());
        let root = s.insert(data).unwrap();
        //print_tree(&*root.to_owned()).unwrap();
        assert_eq!(CHUNK_SIZE, s.size());

        let root_hash = hash(&[0u8; SIZE]);
        assert_eq!(root.get_hash(), &root_hash);

        let zeros_chunk_hash = hash(&[0u8; CHUNK_SIZE]);
        let root_children = (zeros_chunk_hash, hash(&[0u8; CHUNK_SIZE * 2]));
        assert_eq!(root._get_children().unwrap().0.get_hash(), &root_children.0);
        assert_eq!(root._get_children().unwrap().1.get_hash(), &root_children.1);

        let hash_vec = root.flatten();
        assert_eq!(hash_vec.len(), 3);
        assert_eq!(hash_vec[0], zeros_chunk_hash);
        assert_eq!(hash_vec[1], zeros_chunk_hash);
        assert_eq!(hash_vec[2], zeros_chunk_hash);

        let hash_set = root.hashes();
        assert_eq!(hash_set.len(), 1);
        for i in hash_set {
            assert_eq!(i, zeros_chunk_hash);
        }

        let cloned = root.clone_data().unwrap();
        assert_eq!(cloned.len(), SIZE);
        for b in cloned {
            assert_eq!(b, 0u8);
        }
    }

    #[test]
    fn test_hms_2mb() {
        let s = HashMapStorage::default();
        let data = Bytes::from_static(include_bytes!("../../../2mb.random"));
        let len = data.len();
        let root = s.insert(data.clone()).unwrap();
        //print_tree(&*root.to_owned()).unwrap();
        assert!(len >= s.size());

        let cloned = root.clone_data().unwrap();
        for (i, b) in cloned.iter().enumerate() {
            //println!("{} {} {}", i, data[i], *b);
            assert_eq!(data[i], *b);
        }
    }
}
