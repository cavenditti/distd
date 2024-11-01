use std::collections::HashMap;
use std::sync::Arc;

use crate::chunk_storage::ChunkStorage;
use crate::hash::{Hash, HashTreeCapable};

use super::{Node, StorageError};

/// Dead simple in-memory global storage
#[derive(Debug, Default, Clone)]
pub struct HashMapStorage {
    data: HashMap<Hash, Arc<Node>>,
}

impl ChunkStorage for HashMapStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.data.get(hash).cloned()
    }

    fn _insert_chunk(&mut self, hash: Hash, chunk: &[u8]) -> Option<Arc<Node>> {
        //println!("[StorageInsert] Hash: {}, size: {}", hash, size);
        if let Some(raw_chunk) = self.data.get(&hash) {
            return Some(raw_chunk.clone());
        }
        self.data
            .try_insert(
                hash,
                Arc::new(Node::Stored {
                    hash,
                    data: Arc::new(Vec::from(chunk)),
                }),
            )
            .ok()
            .cloned()
    }

    fn _link(&mut self, hash: Hash, left: Arc<Node>, right: Arc<Node>) -> Option<Arc<Node>> {
        /*
        println!(
            "[Storage Link ]: {}: {} + {}",
            hash,
            left.hash(),
            right.hash()
        );
        */
        let size = left.size() + right.size();
        self.data.get(&hash).cloned().or(self
            .data
            .try_insert(
                hash,
                Arc::new(Node::Parent {
                    hash,
                    size,
                    left,
                    right,
                }),
            )
            .ok()
            .map(|x| x.clone()))
    }

    fn chunks(&self) -> Vec<Hash> {
        self.data.keys().copied().collect()
    }

    fn size(&self) -> u64 {
        self.data
            .values()
            .map(|x| match &**x {
                Node::Stored { data, .. } => data.len() as u64,
                Node::Parent { .. } | Node::Skipped { .. } => 0,
            })
            .sum()
    }
}

impl HashTreeCapable<Arc<Node>, crate::error::Error> for HashMapStorage {
    fn func(&mut self, data: &[u8]) -> Result<Arc<Node>, crate::error::Error> {
        Ok(self
            .insert_chunk(data)
            .ok_or(StorageError::ChunkInsertError)?)
    }

    fn merge(&mut self, l: &Arc<Node>, r: &Arc<Node>) -> Result<Arc<Node>, crate::error::Error> {
        Ok(self
            .link(l.clone(), r.clone())
            .ok_or(StorageError::LinkCreation)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::{Bytes, BytesMut};
    use rand::{self, RngCore};
    use test_log::test;

    use crate::{chunks::CHUNK_SIZE, hash::hash};

    #[test]
    fn test_hms_single_chunk_insertion() {
        let mut s = HashMapStorage::default();
        let data = Bytes::from_static(b"very few bytes");
        let len = data.len() as u64;
        s.insert(data);
        assert_eq!(len, s.size());
    }

    #[test]
    /// Multiple chunks, not aligned with `CHUNK_SIZE`
    fn test_hms_insertion() {
        let s = HashMapStorage::default();
        let data = Bytes::from_static(include_bytes!("../../../Cargo.lock"));
        let len = data.len() as u64;
        //let root = s.insert(data).unwrap();
        println!("\nOriginal lenght: {}, stored length: {}", len, s.size());
        //print_tree(&*root.to_owned()).unwrap();
        println!();
        assert!(len >= s.size());
    }

    #[test]
    fn test_hms_deduplicated() {
        const MULT: usize = 3;
        const SIZE: usize = CHUNK_SIZE * MULT;
        let mut s = HashMapStorage::default();
        let data = Bytes::from_static(&[0u8; SIZE]);
        println!(
            "Using {} bytes: CHUNK_SIZE( {CHUNK_SIZE} B ) x {MULT}",
            data.len()
        );

        let root = s.insert(data).unwrap();
        println!("Root node has hash: {}", root.hash());
        assert_eq!(CHUNK_SIZE as u64, s.size());

        let root_hash = hash(&[0u8; SIZE]);
        assert_eq!(root.hash(), &root_hash);

        let zeros_chunk_hash = hash(&[0u8; CHUNK_SIZE]);
        let root_children = (hash(&[0u8; CHUNK_SIZE * 2]), zeros_chunk_hash);
        println!(
            "Root children hashes: {} {}",
            root.children().unwrap().0.hash(),
            root.children().unwrap().1.hash()
        );

        assert_eq!(root.children().unwrap().0.hash(), &root_children.0);
        assert_eq!(root.children().unwrap().1.hash(), &root_children.1);

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

        let cloned = root.clone_data();
        assert_eq!(cloned.len(), SIZE);
        for b in cloned {
            assert_eq!(b, 0u8);
        }
    }

    #[test]
    fn test_hms_2mb() {
        let mut s = HashMapStorage::default();
        let mut data = BytesMut::with_capacity(2_000_000);
        rand::rngs::OsRng.fill_bytes(&mut data);

        let len = data.len() as u64;
        let root = s.insert(data.clone().into()).unwrap();
        //print_tree(&*root.to_owned()).unwrap();
        assert!(len >= s.size());

        let cloned = root.clone_data();
        for (i, b) in cloned.iter().enumerate() {
            //println!("{} {} {}", i, data[i], *b);
            assert_eq!(data[i], *b);
        }
    }
}
