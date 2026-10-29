use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::chunk_storage::ChunkStorage;
use crate::error::Error as CoreError;
use crate::hash::{hash as do_hash, merge_hashes, Hash, HashTreeCapable};
use crate::item::Item;
use bytes::Bytes;

use super::{Node, StorageError};

use redb::{Database, Error, ReadTransaction, ReadableTable, TableDefinition};

mod byte_slice;
use byte_slice::ByteSlice;

const CHUNK_TABLE: TableDefinition<&[u8; 32], ByteSlice> = TableDefinition::new("distd_chunks");
const LINK_TABLE: TableDefinition<&[u8; 32], ([u8; 32], [u8; 32])> =
    TableDefinition::new("distd_links");
const CHUNK_LIST_TABLE: TableDefinition<&[u8; 32], ByteSlice> =
    TableDefinition::new("distd_chunk_lists");

/// Durable content-addressed chunks, links, and ordered revision indexes.
#[derive(Debug, Clone)]
pub struct RedbStorage {
    db: Arc<Database>,
}

impl RedbStorage {
    pub fn new(db_path: &Path) -> Result<Self, Error> {
        let db = Database::create(db_path)?;
        Ok(Self { db: Arc::new(db) })
    }

    fn get_from_snapshot(read_txn: &ReadTransaction, root: &Hash) -> Option<Arc<Node>> {
        enum Visit {
            Enter(Hash),
            Parent { hash: Hash, left: Hash, right: Hash },
        }

        let chunks = read_txn.open_table(CHUNK_TABLE).ok()?;
        let links = read_txn.open_table(LINK_TABLE).ok();
        let mut pending = vec![Visit::Enter(*root)];
        let mut completed: HashMap<Hash, Arc<Node>> = HashMap::new();
        let mut active = HashSet::new();
        while let Some(visit) = pending.pop() {
            match visit {
                Visit::Enter(hash) => {
                    if completed.contains_key(&hash) {
                        continue;
                    }
                    if let Some(data) = chunks.get(hash.as_bytes()).ok()? {
                        completed.insert(
                            hash,
                            Arc::new(Node::Stored {
                                hash,
                                data: Arc::new(data.value().to_vec()),
                            }),
                        );
                        continue;
                    }
                    if !active.insert(hash) {
                        return None; // Corrupt cyclic link graph.
                    }
                    let (left, right) = links.as_ref()?.get(hash.as_bytes()).ok()??.value();
                    let left = Hash::from_bytes(left);
                    let right = Hash::from_bytes(right);
                    pending.push(Visit::Parent { hash, left, right });
                    pending.push(Visit::Enter(right));
                    pending.push(Visit::Enter(left));
                }
                Visit::Parent { hash, left, right } => {
                    let left = completed.get(&left)?.clone();
                    let right = completed.get(&right)?.clone();
                    completed.insert(
                        hash,
                        Arc::new(Node::Parent {
                            hash,
                            size: left.size().checked_add(right.size())?,
                            left,
                            right,
                        }),
                    );
                    active.remove(&hash);
                }
            }
        }
        completed.remove(root)
    }

    fn with_chunk_by_index<T>(
        &self,
        root: &Hash,
        index: u32,
        consume: impl FnOnce(&[u8]) -> T,
    ) -> Option<T> {
        let read_txn = self.db.begin_read().ok()?;
        let lists = read_txn.open_table(CHUNK_LIST_TABLE).ok()?;
        let encoded = lists.get(root.as_bytes()).ok()??;
        let encoded = encoded.value();
        if encoded.len() % 32 != 0 {
            return None;
        }
        let start = usize::try_from(index).ok()?.checked_mul(32)?;
        let end = start.checked_add(32)?;
        let hash = Hash::from_bytes(encoded.get(start..end)?.try_into().ok()?);
        let chunks = read_txn.open_table(CHUNK_TABLE).ok()?;
        let chunk = chunks.get(hash.as_bytes()).ok()??;
        Some(consume(chunk.value()))
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
                leaves.push(payload.slice(offset..offset + length));
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
                    .map(|value| value.value() == data.as_ref());
                if let Some(existing) = existing {
                    if !existing {
                        return Err(StorageError::ImmutableConflict.into());
                    }
                } else {
                    chunks
                        .insert(chunk.hash.as_bytes(), data.as_ref())
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
                .map(|value| value.value() == encoded);
            if let Some(existing) = existing {
                if !existing {
                    return Err(StorageError::ImmutableConflict.into());
                }
            } else {
                lists
                    .insert(item.metadata.root.hash.as_bytes(), encoded.as_slice())
                    .map_err(|error| CoreError::Other(error.to_string()))?;
            }
        }
        write_txn
            .commit()
            .map_err(|error| CoreError::Other(error.to_string()))?;
        Ok(item)
    }
}

impl crate::blob::BlobChunkSource for RedbStorage {
    fn read_chunk(
        &self,
        expected: &crate::chunks::ChunkInfo,
    ) -> std::io::Result<super::node::ArcChunk> {
        let transaction = self.db.begin_read().map_err(std::io::Error::other)?;
        let chunks = match transaction.open_table(CHUNK_TABLE) {
            Ok(chunks) => chunks,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(crate::blob::missing_chunk(expected));
            }
            Err(error) => return Err(std::io::Error::other(error)),
        };
        let data = chunks
            .get(expected.hash.as_bytes())
            .map_err(std::io::Error::other)?
            .ok_or_else(|| crate::blob::missing_chunk(expected))?;
        let bytes = data.value();
        if bytes.len() as u64 != expected.size {
            return Err(crate::blob::invalid_chunk_size());
        }
        Ok(Arc::new(bytes.to_vec()))
    }
}

impl ChunkStorage for RedbStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<Node>> {
        Self::get_from_snapshot(&self.db.begin_read().ok()?, hash)
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
                .map(|value| value.value() == chunk);
            if let Some(existing) = existing {
                if !existing {
                    return Err(StorageError::ImmutableConflict);
                }
            } else {
                table
                    .insert(hash.as_bytes(), chunk)
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
            let bytes = table.get(root.as_bytes()).ok()??;
            let bytes = bytes.value();
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
        self.with_chunk_by_index(root, index, <[u8]>::to_vec)
    }

    fn append_chunk_by_index(
        &self,
        root: &Hash,
        index: u32,
        destination: &mut Vec<u8>,
    ) -> Option<usize> {
        self.with_chunk_by_index(root, index, |chunk| {
            destination.extend_from_slice(chunk);
            chunk.len()
        })
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
    fn legacy_database_remains_readable_in_both_directions() {
        const LEGACY_CHUNKS: TableDefinition<&[u8; 32], Vec<u8>> =
            TableDefinition::new("distd_chunks");
        const LEGACY_LISTS: TableDefinition<&[u8; 32], Vec<u8>> =
            TableDefinition::new("distd_chunk_lists");
        let path = SelfDeletingPath::new(random_path());
        let left = do_hash(b"left");
        let right = do_hash(b"right");
        let parent = merge_hashes(&left, &right);
        let root = merge_hashes(&parent, &left);
        let expected = [left, right, left];
        {
            let db = Database::create(&*path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut chunks = txn.open_table(LEGACY_CHUNKS).unwrap();
                chunks.insert(left.as_bytes(), b"left".to_vec()).unwrap();
                chunks.insert(right.as_bytes(), b"right".to_vec()).unwrap();
                let mut links = txn.open_table(LINK_TABLE).unwrap();
                links
                    .insert(parent.as_bytes(), (*left.as_bytes(), *right.as_bytes()))
                    .unwrap();
                links
                    .insert(root.as_bytes(), (*parent.as_bytes(), *left.as_bytes()))
                    .unwrap();
                txn.open_table(LEGACY_LISTS)
                    .unwrap()
                    .insert(
                        root.as_bytes(),
                        expected
                            .iter()
                            .flat_map(|hash| hash.as_bytes().iter().copied())
                            .collect::<Vec<_>>(),
                    )
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        let fresh = Bytes::from_static(b"new content");
        let item;
        {
            let storage = RedbStorage::new(&path).unwrap();
            let tree = storage.get(&root).unwrap();
            assert_eq!(tree.clone_data().unwrap(), b"leftrightleft");
            assert_eq!(tree.flatten().unwrap(), expected);
            assert_eq!(storage.chunk_list(&root), expected);
            let mut destination = b"prefix:".to_vec();
            for (index, bytes) in [b"left".as_slice(), b"right", b"left"].iter().enumerate() {
                assert_eq!(
                    storage.get_chunk_by_index(&root, index as u32).as_deref(),
                    Some(*bytes)
                );
                assert_eq!(
                    storage.append_chunk_by_index(&root, index as u32, &mut destination),
                    Some(bytes.len())
                );
            }
            assert_eq!(destination, b"prefix:leftrightleft");
            for index in [3, u32::MAX] {
                assert!(storage.get_chunk_by_index(&root, index).is_none());
                assert!(storage
                    .append_chunk_by_index(&root, index, &mut destination)
                    .is_none());
                assert_eq!(destination, b"prefix:leftrightleft");
            }
            item = storage
                .prepare_revision(
                    "fresh".into(),
                    "fresh".into(),
                    "fresh.bin".into(),
                    0,
                    None,
                    fresh.clone(),
                    crate::chunks::ChunkAlgorithm::fixed_aligned(4, 1),
                )
                .unwrap();
            storage
                .commit_revision(item.clone(), fresh.clone())
                .unwrap();
        }
        {
            let db = Database::open(&*path).unwrap();
            let txn = db.begin_read().unwrap();
            let chunks = txn.open_table(LEGACY_CHUNKS).unwrap();
            for (chunk, bytes) in item.chunks.iter().zip(fresh.chunks(4)) {
                assert_eq!(
                    chunks.get(chunk.hash.as_bytes()).unwrap().unwrap().value(),
                    bytes
                );
            }
            let lists = txn.open_table(LEGACY_LISTS).unwrap();
            let encoded = lists
                .get(item.metadata.root.hash.as_bytes())
                .unwrap()
                .unwrap()
                .value();
            assert_eq!(
                encoded,
                item.chunks
                    .iter()
                    .flat_map(|chunk| chunk.hash.as_bytes().iter().copied())
                    .collect::<Vec<_>>()
            );
        }
        let storage = RedbStorage::new(&path).unwrap();
        storage.verify_committed_revision(&item).unwrap();
    }

    #[test]
    fn tree_reads_reuse_shared_nodes_within_one_snapshot() {
        let storage = make_redb_storage();
        let leaf = storage.insert_chunk(b"shared").unwrap();
        let parent = storage.link(leaf.clone(), leaf).unwrap();
        let root = storage.link(parent.clone(), parent).unwrap();
        let loaded = storage.get(root.hash()).unwrap();
        let (left, right) = loaded.children().unwrap();
        assert!(Arc::ptr_eq(left, right));
        let (left, right) = left.children().unwrap();
        assert!(Arc::ptr_eq(left, right));
        assert_eq!(loaded.clone_data().unwrap(), b"sharedsharedsharedshared");

        let snapshot = storage.db.begin_read().unwrap();
        let txn = storage.db.begin_write().unwrap();
        txn.open_table(CHUNK_TABLE)
            .unwrap()
            .remove(left.hash().as_bytes())
            .unwrap();
        txn.commit().unwrap();
        assert!(RedbStorage::get_from_snapshot(&snapshot, root.hash()).is_some());
        assert!(storage.get(root.hash()).is_none());
    }

    #[test]
    fn invalid_index_or_missing_payload_does_not_append_bytes() {
        let storage = make_redb_storage();
        let leaf = storage.insert_chunk(b"present").unwrap();
        let root = do_hash(b"index");
        let missing = do_hash(b"absent");
        let mut destination = b"unchanged".to_vec();
        for encoded in [
            [leaf.hash().as_bytes().as_slice(), &[0]].concat(),
            missing.as_bytes().to_vec(),
        ] {
            let txn = storage.db.begin_write().unwrap();
            txn.open_table(CHUNK_LIST_TABLE)
                .unwrap()
                .insert(root.as_bytes(), encoded.as_slice())
                .unwrap();
            txn.commit().unwrap();
            assert!(storage.get_chunk_by_index(&root, 0).is_none());
            assert!(storage
                .append_chunk_by_index(&root, 0, &mut destination)
                .is_none());
            assert_eq!(destination, b"unchanged");
        }
    }

    #[test]
    fn tree_read_rejects_cycles_and_size_overflow() {
        let storage = make_redb_storage();
        let leaf = storage.insert_chunk(b"x").unwrap();
        let cycle = do_hash(b"cycle");
        let txn = storage.db.begin_write().unwrap();
        let mut root = *leaf.hash();
        {
            let mut links = txn.open_table(LINK_TABLE).unwrap();
            links
                .insert(
                    cycle.as_bytes(),
                    (*cycle.as_bytes(), *leaf.hash().as_bytes()),
                )
                .unwrap();
            for _ in 0..64 {
                let parent = merge_hashes(&root, &root);
                links
                    .insert(parent.as_bytes(), (*root.as_bytes(), *root.as_bytes()))
                    .unwrap();
                root = parent;
            }
        }
        txn.commit().unwrap();
        assert!(storage.get(&cycle).is_none());
        assert!(storage.get(&root).is_none());
    }

    #[test]
    fn failed_batched_commit_does_not_publish_partial_chunks() {
        let storage = make_redb_storage();
        let data = Bytes::from_static(b"aaaabbbb");
        let item = storage
            .prepare_revision(
                "artifact".into(),
                "artifact".into(),
                "artifact.bin".into(),
                0,
                None,
                data.clone(),
                crate::chunks::ChunkAlgorithm::fixed_aligned(4, 1),
            )
            .unwrap();
        let first = item.chunks[0].hash;
        let root = item.metadata.root.hash;
        let txn = storage.db.begin_write().unwrap();
        txn.open_table(CHUNK_TABLE)
            .unwrap()
            .insert(item.chunks[1].hash.as_bytes(), b"corrupt".as_slice())
            .unwrap();
        txn.commit().unwrap();
        assert!(matches!(
            storage.commit_revision(item, data),
            Err(CoreError::Storage(StorageError::ImmutableConflict))
        ));
        assert!(storage.get(&first).is_none());
        assert!(storage.get(&root).is_none());
        assert!(storage.chunk_list(&root).is_empty());
    }

    #[test]
    fn empty_directory_files_keep_their_ordered_chunk_entries() {
        let storage = make_redb_storage();
        let files = vec![
            (PathBuf::from("z.bin"), Bytes::new()),
            (PathBuf::from("a.bin"), Bytes::from_static(b"data")),
            (PathBuf::from("m.bin"), Bytes::new()),
        ];
        let item = storage
            .prepare_revision_from_files(
                "artifact".into(),
                "artifact".into(),
                "directory".into(),
                0,
                None,
                files.clone(),
                crate::chunks::ChunkAlgorithm::default(),
            )
            .unwrap();
        storage
            .commit_revision_from_files(item.clone(), files)
            .unwrap();
        storage.verify_committed_revision(&item).unwrap();
        let root = item.metadata.root.hash;
        assert_eq!(storage.get_chunk_by_index(&root, 0).unwrap(), b"data");
        assert_eq!(storage.get_chunk_by_index(&root, 1).unwrap(), b"");
        assert_eq!(storage.get_chunk_by_index(&root, 2).unwrap(), b"");
    }

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
