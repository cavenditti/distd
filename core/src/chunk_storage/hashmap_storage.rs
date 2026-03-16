use rustc_hash::FxHashMap;
use std::sync::{Arc, RwLock};

use crate::chunk_storage::ChunkStorage;
use crate::hash::{Hash, HashTreeCapable};

use super::{Node, StorageError};

/// Dead simple in-memory global storage
#[derive(Debug, Default)]
pub struct HashMapStorage {
    data: Arc<RwLock<FxHashMap<Hash, Arc<Node>>>>,
}

/// Cloning shares the underlying map (Arc clone).
impl Clone for HashMapStorage {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
        }
    }
}

impl ChunkStorage for HashMapStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.data.read().unwrap().get(hash).cloned()
    }

    fn store_chunk(&self, hash: Hash, chunk: &[u8]) -> Result<Arc<Node>, StorageError> {
        let mut data = self.data.write().unwrap();
        if let Some(node) = data.get(&hash) {
            return Ok(node.clone());
        }
        let node = Arc::new(Node::Stored {
            hash,
            data: Arc::new(Vec::from(chunk)),
        });
        data.insert(hash, node.clone());
        Ok(node)
    }

    fn store_link(
        &self,
        hash: Hash,
        left: Arc<Node>,
        right: Arc<Node>,
    ) -> Result<Arc<Node>, StorageError> {
        let mut data = self.data.write().unwrap();
        if let Some(node) = data.get(&hash) {
            return Ok(node.clone());
        }
        let size = left.size() + right.size();
        let node = Arc::new(Node::Parent {
            hash,
            size,
            left,
            right,
        });
        data.insert(hash, node.clone());
        Ok(node)
    }

    fn chunks(&self) -> Vec<Hash> {
        self.data.read().unwrap().keys().copied().collect()
    }

    fn size(&self) -> u64 {
        self.data
            .read()
            .unwrap()
            .values()
            .map(|x| match &**x {
                Node::Stored { data, .. } => data.len() as u64,
                Node::Parent { .. } | Node::Skipped { .. } => 0,
            })
            .sum()
    }
}

impl HashTreeCapable<Arc<Node>, crate::error::Error> for HashMapStorage {
    fn func(&self, data: &[u8]) -> Result<Arc<Node>, crate::error::Error> {
        self.insert_chunk(data).map_err(Into::into)
    }

    fn merge(&self, l: &Arc<Node>, r: &Arc<Node>) -> Result<Arc<Node>, crate::error::Error> {
        self.link(l.clone(), r.clone()).map_err(Into::into)
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
