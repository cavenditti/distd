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

    fn _insert_chunk(&mut self, hash: Hash, chunk: &[u8]) -> Option<Arc<Node>> {
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

    fn _link(&mut self, hash: Hash, left: Arc<Node>, right: Arc<Node>) -> Option<Arc<Node>> {
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
            .map(|v| v.unwrap().0.value().clone())
            .map(|v| Hash::from_bytes(v))
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
    use std::path::PathBuf;

    use super::*;

    use bytes::{Bytes, BytesMut};
    use rand::{self, RngCore};
    use test_log::test;

    use crate::{chunks::CHUNK_SIZE, hash::hash};

    #[test]
    fn test_redb_single_chunk_insertion() {
        let mut s = RedbStorage::new(&PathBuf::from("redb.1.test")).unwrap();
        let data = Bytes::from_static(b"very few bytes");
        let len = data.len() as u64;
        s.insert(data);
        assert_eq!(len, s.size())
    }

    #[test]
    /// Multiple chunks, not aligned with `CHUNK_SIZE`
    fn test_redb_insertion() {
        let s = RedbStorage::new(&PathBuf::from("redb.2.test")).unwrap();
        let data = Bytes::from_static(include_bytes!("../../../Cargo.lock"));
        let len = data.len() as u64;
        //let root = s.insert(data).unwrap();
        println!("\nOriginal lenght: {}, stored length: {}", len, s.size());
        //print_tree(&*root.to_owned()).unwrap();
        println!();
        assert!(len >= s.size());
    }

    #[test]
    fn test_redb_deduplicated() {
        const MULT: usize = 3;
        const SIZE: usize = CHUNK_SIZE * MULT;
        let mut s = RedbStorage::new(&PathBuf::from("redb.3.test")).unwrap();
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
    fn test_redb_2mb() {
        let mut s = RedbStorage::new(&PathBuf::from("redb.4.test")).unwrap();
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
