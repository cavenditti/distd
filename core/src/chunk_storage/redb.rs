use std::path::Path;
use std::sync::Arc;

use crate::chunk_storage::ChunkStorage;
use crate::hash::{Hash, HashTreeCapable};

use super::{Node, StorageError};

use redb::{Database, Error, ReadTransaction, ReadableTable, TableDefinition};

const CHUNK_TABLE: TableDefinition<&[u8; 32], Vec<u8>> = TableDefinition::new("distd_chunks");
const LINK_TABLE: TableDefinition<&[u8; 32], ([u8; 32], [u8; 32])> =
    TableDefinition::new("distd_links");

/// Dead simple in-memory global storage
#[derive(Debug, Clone)]
pub struct RedbStorage {
    db: Arc<Database>, //<Hash, Arc<Node>>,
}

impl RedbStorage {
    pub fn new(db_path: &Path) -> Result<Self, Error> {
        let db = Database::create(db_path)?;
        Ok(Self { db: Arc::new(db) })
    }

    fn get_stored_node(&self, read_txn: &ReadTransaction, hash: &Hash) -> Option<Node> {
        read_txn
            .open_table(CHUNK_TABLE)
            .ok()
            .and_then(|table| table.get(&hash.as_bytes()).ok()?)
            .map(|guard| guard.value())
            .map(|v| Node::Stored {
                hash: *hash,
                data: Arc::new(v),
            })
    }

    fn get_parent_node(&self, read_txn: &ReadTransaction, hash: &Hash) -> Option<Node> {
        read_txn
            .open_table(LINK_TABLE)
            .ok()
            .and_then(|table| table.get(hash.as_bytes()).ok()?)
            .map(|guard| guard.value())
            .and_then(|v| {
                let left = self.get(&Hash::from_bytes(v.0))?;
                let right = self.get(&Hash::from_bytes(v.1))?;
                {
                    Some(Node::Parent {
                        hash: *hash,
                        size: left.size() + right.size(),
                        left,
                        right,
                    })
                }
            })
    }
}

impl ChunkStorage for RedbStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.db
            .begin_read()
            .ok()
            .and_then(|read_txn| {
                self.get_stored_node(&read_txn, hash)
                    .or(self.get_parent_node(&read_txn, hash))
            })
            .map(Arc::new)
    }

    fn store_chunk(&mut self, hash: Hash, chunk: &[u8]) -> Option<Arc<Node>> {
        let write_txn = self.db.begin_write().ok()?;
        {
            let mut table = write_txn.open_table(CHUNK_TABLE).ok()?;
            table.insert(hash.as_bytes(), Vec::from(chunk)).ok()?;
        }
        write_txn.commit().ok()?;
        Some(Arc::new(Node::Stored {
            hash,
            data: Arc::new(Vec::from(chunk)),
        }))
    }

    fn store_link(&mut self, hash: Hash, left: Arc<Node>, right: Arc<Node>) -> Option<Arc<Node>> {
        let size = left.size() + right.size();
        let write_txn = self.db.begin_write().ok()?;
        {
            let mut table = write_txn.open_table(LINK_TABLE).ok()?;
            table
                .insert(
                    hash.as_bytes(),
                    (*left.hash().as_bytes(), *right.hash().as_bytes()),
                )
                .ok()?;
        }
        write_txn.commit().ok()?;
        Some(Arc::new(Node::Parent {
            hash,
            size,
            left,
            right,
        }))
    }

    fn chunks(&self) -> Vec<Hash> {
        self.db
            .begin_read()
            .unwrap()
            .open_table(CHUNK_TABLE)
            .unwrap()
            .iter()
            .unwrap()
            .map(|v| *v.unwrap().0.value())
            .map(Hash::from_bytes)
            .collect()
    }

    fn size(&self) -> u64 {
        fn get_size(redb_storage: &RedbStorage) -> Option<u64> {
            Some(
                redb_storage
                    .db
                    .begin_read()
                    .ok()?
                    .open_table(CHUNK_TABLE)
                    .ok()?
                    .iter()
                    .ok()?
                    .map(|v| v.unwrap().1.value().len() as u64)
                    .sum(),
            )
        }
        get_size(self).unwrap_or(0)
    }
}

impl HashTreeCapable<Arc<Node>, crate::error::Error> for RedbStorage {
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

    use crate::utils::testing::{random_path, SelfDeletingPath};

    fn make_redb_storage() -> RedbStorage {
        let p = SelfDeletingPath::new(random_path());
        RedbStorage::new(&p).unwrap()
    }

    crate::chunk_storage::tests::chunk_storage_tests!(RedbStorage, make_redb_storage);
}
