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

    use test_log::test;

    fn make_hashmap_storage() -> HashMapStorage {
        HashMapStorage::default()
    }

    crate::chunk_storage::tests::chunk_storage_tests!(HashMapStorage, make_hashmap_storage);
}
