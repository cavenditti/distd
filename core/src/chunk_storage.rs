use std::{path::PathBuf, sync::Arc};

use blake3::Hash;
use bytes::Bytes;
pub use stored_chunk_ref::StoredChunkRef;

use crate::{
    chunks::CHUNK_SIZE,
    hash::merge_hashes,
    item::{Item, ItemName},
};

pub mod fs_storage;
pub mod hashmap_storage;
pub mod stored_chunk_ref;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Unknown storage size")]
    UnknownSize,
    #[error("Cannot insert chunk in data store")]
    UnknownChunkInsertError(#[from] std::io::Error),
}

/// Defines a backend used to store hashes and chunks ad key-value pairs
pub trait ChunkStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<StoredChunkRef>>;
    fn _insert_chunk(&self, hash: Hash, chunk: &[u8]) -> Option<Arc<StoredChunkRef>>;
    fn _link(
        &self,
        hash: Hash,
        left: Arc<StoredChunkRef>,
        right: Arc<StoredChunkRef>,
    ) -> Option<Arc<StoredChunkRef>>;

    fn chunks(&self) -> Vec<Hash>;

    /// Allocated size for all chunks, in bytes
    /// This only counts actual chunks size, excluding any auxiliary structure used by storage backend/adapter
    fn size(&self) -> usize;

    //fn drop(hash: Hash); // TODO

    fn insert_chunk(&self, chunk: &[u8]) -> Option<Arc<StoredChunkRef>> {
        let hash = blake3::hash(chunk);
        self._insert_chunk(hash, chunk)
            .inspect(|x| assert!(*x.hash() == hash))
    }

    fn link(
        &self,
        left: Arc<StoredChunkRef>,
        right: Arc<StoredChunkRef>,
    ) -> Option<Arc<StoredChunkRef>> {
        let hash = merge_hashes(left.hash(), right.hash());
        self._link(hash, left, right)
            .inspect(|x| assert!(*x.hash() == hash))
    }

    /// Insert bytes into the storage returning the associated hash tree
    fn insert(&self, data: Bytes) -> Option<Arc<StoredChunkRef>>
    where
        Self: Sized,
    {
        fn partial_tree(
            storage: &dyn ChunkStorage,
            slices: &[&[u8]],
        ) -> Option<Arc<StoredChunkRef>> {
            /*
            println!(
                "[StorageChunks] {} {:?}",
                slices.len(),
                slices.iter().map(|x| x.len()).collect::<Vec<usize>>()
            );
            */
            match slices.len() {
                0 => storage.insert_chunk(b""), // Transparently andle empty files too
                1 => storage.insert_chunk(slices[0]),
                /*
                                 _ => storage.link(
                                    partial_tree(storage, slices, &[])?,
                                    partial_tree(storage, &[], slices)?,
                                ),
                */
                _ => storage.link(
                    partial_tree(storage, &slices[..slices.len() / 2])?,
                    partial_tree(storage, &slices[slices.len() / 2..])?,
                ),
            }
        }

        let (chunks, remainder) = data.as_chunks::<CHUNK_SIZE>();
        let mut slices = chunks.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(); // FIXME is this zero copy?
        if !remainder.is_empty() {
            slices.push(remainder);
        }
        partial_tree(self, slices.as_slice())
    }

    /// Create a new Item from its metadata and Bytes
    /// This is the preferred way to create a new Item
    fn create_item(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        file: Bytes,
    ) -> Option<Item>
    where
        Self: Sized,
    {
        let hash_tree = self.insert(file)?;
        Some(Item::new(name, path, revision, description, hash_tree))
    }
}
