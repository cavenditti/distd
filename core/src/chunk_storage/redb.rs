use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::chunk_storage::ChunkStorage;
use crate::error::Error as CoreError;
use crate::hash::{hash as do_hash, merge_hashes, Hash, HashTreeCapable};
use crate::item::Item;
use bytes::Bytes;

use super::{Node, StorageError};

use redb::{Database, Error, ReadTransaction, ReadableTable, TableDefinition};

const CHUNK_TABLE: TableDefinition<&[u8; 32], Vec<u8>> = TableDefinition::new("distd_chunks");
const LINK_TABLE: TableDefinition<&[u8; 32], ([u8; 32], [u8; 32])> =
    TableDefinition::new("distd_links");
const CHUNK_LIST_TABLE: TableDefinition<&[u8; 32], Vec<u8>> =
    TableDefinition::new("distd_chunk_lists");

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

    fn commit_prepared_payloads(&self, item: Item, payloads: &[Bytes]) -> Result<Item, CoreError> {
        let mut leaves = Vec::with_capacity(item.chunks.len());
        for payload in payloads {
            let boundaries = if payload.is_empty() {
                vec![(0, 0)]
            } else {
                item.manifest
                    .chunk_algorithm
                    .chunk_boundaries(payload.as_ref())
            };
            for (offset, length) in boundaries {
                leaves.push(Bytes::copy_from_slice(
                    &payload.as_ref()[offset..offset + length],
                ));
            }
        }
        if leaves.len() != item.chunks.len()
            || leaves.iter().zip(&item.chunks).any(|(data, chunk)| {
                data.len() as u64 != chunk.size || do_hash(data.as_ref()) != chunk.hash
            })
        {
            return Err(CoreError::Other(
                "prepared redb revision does not match supplied payloads".to_string(),
            ));
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|error| CoreError::Other(error.to_string()))?;
        {
            let mut chunks = write_txn
                .open_table(CHUNK_TABLE)
                .map_err(|error| CoreError::Other(error.to_string()))?;
            let mut links = write_txn
                .open_table(LINK_TABLE)
                .map_err(|error| CoreError::Other(error.to_string()))?;

            for (data, chunk) in leaves.iter().zip(&item.chunks) {
                if links
                    .get(chunk.hash.as_bytes())
                    .map_err(|error| CoreError::Other(error.to_string()))?
                    .is_some()
                {
                    return Err(StorageError::ImmutableConflict.into());
                }
                let existing = chunks
                    .get(chunk.hash.as_bytes())
                    .map_err(|error| CoreError::Other(error.to_string()))?
                    .map(|value| value.value());
                if let Some(existing) = existing {
                    if existing.as_slice() != data.as_ref() {
                        return Err(StorageError::ImmutableConflict.into());
                    }
                } else {
                    chunks
                        .insert(chunk.hash.as_bytes(), data.to_vec())
                        .map_err(|error| CoreError::Other(error.to_string()))?;
                }
            }

            let mut partials = item
                .chunks
                .iter()
                .map(|chunk| (chunk.hash, chunk.size))
                .collect::<Vec<_>>();
            while partials.len() > 1 {
                let n = partials.len();
                for (to, index) in (0..n - 1).step_by(2).enumerate() {
                    let left = partials[index];
                    let right = partials[index + 1];
                    let hash = merge_hashes(&left.0, &right.0);
                    let size = left.1.checked_add(right.1).ok_or_else(|| {
                        CoreError::Other("redb revision size overflows u64".to_string())
                    })?;
                    if chunks
                        .get(hash.as_bytes())
                        .map_err(|error| CoreError::Other(error.to_string()))?
                        .is_some()
                    {
                        return Err(StorageError::ImmutableConflict.into());
                    }
                    let expected = (*left.0.as_bytes(), *right.0.as_bytes());
                    let existing = links
                        .get(hash.as_bytes())
                        .map_err(|error| CoreError::Other(error.to_string()))?
                        .map(|value| value.value());
                    if let Some(existing) = existing {
                        if existing != expected {
                            return Err(StorageError::ImmutableConflict.into());
                        }
                    } else {
                        links
                            .insert(hash.as_bytes(), expected)
                            .map_err(|error| CoreError::Other(error.to_string()))?;
                    }
                    partials[to] = (hash, size);
                }
                let half = n / 2;
                if n % 2 != 0 {
                    partials.swap(half, n - 1);
                    partials.truncate(half + 1);
                } else {
                    partials.truncate(half);
                }
            }
            if partials.as_slice() != [(item.metadata.root.hash, item.metadata.root.size)] {
                return Err(CoreError::Other(
                    "redb revision root does not match prepared metadata".to_string(),
                ));
            }
        }
        {
            let mut lists = write_txn
                .open_table(CHUNK_LIST_TABLE)
                .map_err(|error| CoreError::Other(error.to_string()))?;
            let encoded = item
                .chunks
                .iter()
                .flat_map(|chunk| chunk.hash.as_bytes().iter().copied())
                .collect::<Vec<_>>();
            let existing = lists
                .get(item.metadata.root.hash.as_bytes())
                .map_err(|error| CoreError::Other(error.to_string()))?
                .map(|value| value.value());
            if let Some(existing) = existing {
                if existing != encoded {
                    return Err(StorageError::ImmutableConflict.into());
                }
            } else {
                lists
                    .insert(item.metadata.root.hash.as_bytes(), encoded)
                    .map_err(|error| CoreError::Other(error.to_string()))?;
            }
        }
        write_txn
            .commit()
            .map_err(|error| CoreError::Other(error.to_string()))?;
        Ok(item)
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

    fn store_chunk(&self, hash: Hash, chunk: &[u8]) -> Result<Arc<Node>, StorageError> {
        if do_hash(chunk) != hash {
            return Err(StorageError::ImmutableConflict);
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
        {
            let links = write_txn
                .open_table(LINK_TABLE)
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
            if links
                .get(hash.as_bytes())
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
                .is_some()
            {
                return Err(StorageError::ImmutableConflict);
            }
        }
        {
            let mut table = write_txn
                .open_table(CHUNK_TABLE)
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
            let existing = table
                .get(hash.as_bytes())
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
                .map(|value| value.value());
            if let Some(existing) = existing {
                if existing.as_slice() != chunk {
                    return Err(StorageError::ImmutableConflict);
                }
            } else {
                table
                    .insert(hash.as_bytes(), Vec::from(chunk))
                    .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
        Ok(Arc::new(Node::Stored {
            hash,
            data: Arc::new(Vec::from(chunk)),
        }))
    }

    fn store_link(
        &self,
        hash: Hash,
        left: Arc<Node>,
        right: Arc<Node>,
    ) -> Result<Arc<Node>, StorageError> {
        if merge_hashes(left.hash(), right.hash()) != hash {
            return Err(StorageError::ImmutableConflict);
        }
        let size = left.size() + right.size();
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
        {
            let chunks = write_txn
                .open_table(CHUNK_TABLE)
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
            if chunks
                .get(hash.as_bytes())
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
                .is_some()
            {
                return Err(StorageError::ImmutableConflict);
            }
        }
        {
            let mut table = write_txn
                .open_table(LINK_TABLE)
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
            let expected = (*left.hash().as_bytes(), *right.hash().as_bytes());
            let existing = table
                .get(hash.as_bytes())
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
                .map(|value| value.value());
            if let Some(existing) = existing {
                if existing != expected {
                    return Err(StorageError::ImmutableConflict);
                }
            } else {
                table
                    .insert(hash.as_bytes(), expected)
                    .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
        Ok(Arc::new(Node::Parent {
            hash,
            size,
            left,
            right,
        }))
    }

    fn chunks(&self) -> Vec<Hash> {
        let Ok(read_txn) = self.db.begin_read() else {
            return Vec::new();
        };
        let Ok(table) = read_txn.open_table(CHUNK_TABLE) else {
            return Vec::new();
        };
        let Ok(iter) = table.iter() else {
            return Vec::new();
        };
        iter.filter_map(|v| v.ok())
            .map(|v| Hash::from_bytes(*v.0.value()))
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

    fn chunk_list(&self, root: &Hash) -> Vec<Hash> {
        let direct = self.db.begin_read().ok().and_then(|read_txn| {
            let table = read_txn.open_table(CHUNK_LIST_TABLE).ok()?;
            let bytes = table.get(root.as_bytes()).ok()??.value();
            if bytes.len() % 32 != 0 {
                return None;
            }
            Some(
                bytes
                    .chunks_exact(32)
                    .map(|chunk| Hash::from_bytes(chunk.try_into().expect("32-byte chunk hash")))
                    .collect(),
            )
        });
        direct.unwrap_or_else(|| {
            self.get(root)
                .and_then(|node| node.flatten().ok())
                .unwrap_or_default()
        })
    }

    fn get_chunk_by_index(&self, root: &Hash, index: u32) -> Option<Vec<u8>> {
        let read_txn = self.db.begin_read().ok()?;
        let lists = read_txn.open_table(CHUNK_LIST_TABLE).ok()?;
        let encoded = lists.get(root.as_bytes()).ok()??;
        let start = usize::try_from(index).ok()?.checked_mul(32)?;
        let end = start.checked_add(32)?;
        let hash = Hash::from_bytes(encoded.value().get(start..end)?.try_into().ok()?);
        let chunks = read_txn.open_table(CHUNK_TABLE).ok()?;
        Some(chunks.get(hash.as_bytes()).ok()??.value())
    }

    fn commit_revision(&self, item: Item, file: Bytes) -> Result<Item, CoreError>
    where
        Self: Sized,
    {
        let expected = self.prepare_revision(
            item.metadata.artifact_id.clone(),
            item.metadata.name.clone(),
            item.metadata.path.clone(),
            item.metadata.revision,
            item.metadata.description.clone(),
            file.clone(),
            item.manifest.chunk_algorithm,
        )?;
        if !super::same_revision(&item, &expected) {
            return Err(CoreError::Other(
                "prepared redb revision does not match supplied bytes".to_string(),
            ));
        }
        self.commit_prepared_payloads(item, &[file])
    }

    fn commit_revision_from_files(
        &self,
        item: Item,
        mut files: Vec<(PathBuf, Bytes)>,
    ) -> Result<Item, CoreError>
    where
        Self: Sized,
    {
        let expected = self.prepare_revision_from_files(
            item.metadata.artifact_id.clone(),
            item.metadata.name.clone(),
            item.metadata.path.clone(),
            item.metadata.revision,
            item.metadata.description.clone(),
            files.clone(),
            item.manifest.chunk_algorithm,
        )?;
        if !super::same_revision(&item, &expected) {
            return Err(CoreError::Other(
                "prepared redb directory revision does not match supplied files".to_string(),
            ));
        }
        files.sort_by(|left, right| left.0.cmp(&right.0));
        let payloads = files
            .into_iter()
            .map(|(_, bytes)| bytes)
            .collect::<Vec<_>>();
        self.commit_prepared_payloads(item, &payloads)
    }
}

impl HashTreeCapable<Arc<Node>, crate::error::Error> for RedbStorage {
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

    use crate::utils::testing::{random_path, SelfDeletingPath};

    fn make_redb_storage() -> RedbStorage {
        let p = SelfDeletingPath::new(random_path());
        RedbStorage::new(&p).unwrap()
    }

    crate::chunk_storage::tests::chunk_storage_tests!(RedbStorage, make_redb_storage);

    #[test]
    fn rejects_mismatched_immutable_keys() {
        let storage = make_redb_storage();
        assert!(matches!(
            storage.store_chunk(do_hash(b"different"), b"original"),
            Err(StorageError::ImmutableConflict)
        ));

        let left = storage.insert_chunk(b"left").unwrap();
        let right = storage.insert_chunk(b"right").unwrap();
        assert!(matches!(
            storage.store_link(do_hash(b"not a parent"), left, right),
            Err(StorageError::ImmutableConflict)
        ));
    }

    #[test]
    fn batched_revision_commit_retains_direct_chunk_index() {
        let storage = make_redb_storage();
        let algorithm = crate::chunks::ChunkAlgorithm::default();
        let bytes = Bytes::from(vec![7u8; crate::chunks::CHUNK_SIZE * 2]);
        let item = storage
            .prepare_revision(
                "stable".to_string(),
                "display".to_string(),
                PathBuf::from("logical.bin"),
                0,
                None,
                bytes.clone(),
                algorithm,
            )
            .unwrap();
        let root = item.metadata.root.hash;
        let expected_hashes = item
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect::<Vec<_>>();
        storage.commit_revision(item, bytes.clone()).unwrap();

        assert_eq!(storage.chunk_list(&root), expected_hashes);
        assert_eq!(
            storage.get_chunk_by_index(&root, 0),
            Some(bytes[..crate::chunks::CHUNK_SIZE].to_vec())
        );
        assert_eq!(
            storage.get_chunk_by_index(&root, 1),
            Some(bytes[crate::chunks::CHUNK_SIZE..].to_vec())
        );
    }
}
