use std::fmt::Write;
use std::{collections::HashSet, path::PathBuf, sync::Arc};

use bytes::Bytes;
pub use node::Node;
use tokio_stream::{Stream, StreamExt};

use crate::error::InvalidParameter;
use crate::hash::{hash, Hash};
use crate::proto::SerializedTree;
use crate::{
    chunks::CHUNK_SIZE,
    hash::merge_hashes,
    item::{Item, Name as ItemName},
};

pub mod fs_storage;
pub mod hashmap_storage;
pub mod node;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Unknown storage size")]
    UnknownSize,

    #[error("Cannot insert chunk in data store")]
    UnknownChunkInsertError(#[from] std::io::Error),

    #[error("Cannot reconstruct tree from storage")]
    TreeReconstruct,
}

/// Defines a backend used to store hashes and chunks ad key-value pairs
pub trait ChunkStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<Node>>;
    fn _insert_chunk(&mut self, hash: Hash, chunk: &[u8]) -> Option<Arc<Node>>;
    fn _link(
        &mut self,
        hash: Hash,
        left: Arc<Node>,
        right: Arc<Node>,
    ) -> Option<Arc<Node>>;

    fn chunks(&self) -> Vec<Hash>;

    /// Allocated size for all chunks, in bytes
    /// This only counts actual chunks size, excluding any auxiliary structure used by storage backend/adapter
    fn size(&self) -> u64;

    //fn drop(hash: Hash); // TODO

    fn insert_chunk(&mut self, chunk: &[u8]) -> Option<Arc<Node>> {
        let hash = hash(chunk);
        tracing::trace!("Insert chunk {hash}, {} bytes", chunk.len());

        self._insert_chunk(hash, chunk)
            .inspect(|x| assert!(*x.hash() == hash))
    }

    fn link(
        &mut self,
        left: Arc<Node>,
        right: Arc<Node>,
    ) -> Option<Arc<Node>> {
        let hash = merge_hashes(left.hash(), right.hash());
        tracing::trace!("Link {} {} → {}", left.hash(), right.hash(), hash);
        self._link(hash, left, right)
            .inspect(|x| assert!(*x.hash() == hash))
    }

    /// Insert bytes into the storage returning the associated hash tree
    fn insert(&mut self, data: Bytes) -> Option<Arc<Node>>
    where
        Self: Sized,
    {
        fn partial_tree(
            storage: &mut dyn ChunkStorage,
            slices: &[&[u8]],
        ) -> Option<Arc<Node>> {
            /*
            tracing::trace!(
                "{} {:?}",
                slices.len(),
                slices.iter().map(|x| x.len()).collect::<Vec<usize>>()
            );
            */
            match slices.len() {
                0 => storage.insert_chunk(b""), // Transparently handle empty files too
                1 => storage.insert_chunk(slices[0]),
                _ => {
                    let l = partial_tree(storage, &slices[..slices.len() / 2])?;
                    let r = partial_tree(storage, &slices[slices.len() / 2..])?;
                    storage.link(l, r)
                }
            }
        }

        if data.len() > 32 {
            tracing::trace!(
                "Inserting: {}..{}, {}B",
                data[..16].iter().fold(String::new(), |mut s, b| {
                    write!(s, "{b:x}").unwrap();
                    s
                }),
                data[data.len() - 16..]
                    .iter()
                    .fold(String::new(), |mut s, b| {
                        write!(s, "{b:x}").unwrap();
                        s
                    }),
                data.len()
            );
        } else {
            tracing::trace!("Inserting: {:?}, {}B", data, data.len());
        };

        let chunks = data.chunks(CHUNK_SIZE).collect::<Vec<&[u8]>>();

        tracing::trace!("{} chunks", chunks.len());
        partial_tree(self, &chunks)
    }

    /// Create a new Item from its metadata and Bytes
    /// This is the preferred way to create a new Item
    fn create_item(
        &mut self,
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
        Some(Item::new(name, path, revision, description, &hash_tree))
    }

    /// Build a new Item from its metadata and root node
    fn build_item(
        &mut self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        root: Arc<Node>,
    ) -> Option<Item>
    where
        Self: Sized,
    {
        Some(Item::new(name, path, revision, description, &root))
    }

    /// Build a new Item from its metadata and a streaming of nodes
    fn receive_item<T>(
        &mut self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        mut stream: T,
        //) -> Result<Item, crate::error::Error>
    ) -> impl std::future::Future<Output = Result<Item, crate::error::Error>> + Send
    where
        Self: Sized + Send,
        T: Stream<Item = SerializedTree> + std::marker::Unpin + Send,
    {
        async move {
            let mut n = None; // final node
            let mut i = 0; // node counter
            while let Some(node) = stream.next().await {
                tracing::trace!(
                    "Received {} bytes ({}..{})",
                    node.bitcode_hashtree.len(),
                    &node.bitcode_hashtree[..8]
                        .iter()
                        .map(|x| format!("{:02x}", x))
                        .collect::<String>(),
                    &node.bitcode_hashtree[node.bitcode_hashtree.len() - 8..]
                        .iter()
                        .map(|x| format!("{:02x}", x))
                        .collect::<String>(),
                );
                let deser = bitcode::deserialize(&node.bitcode_hashtree)
                    .map_err(InvalidParameter::Bitcode)?;
                tracing::trace!("Deserialized: {:?}", deser);
                n = Some(
                    self.try_fill_in(&deser)
                        .ok_or(StorageError::TreeReconstruct)?,
                );
                i += 1;
            }

            let n = n.ok_or(StorageError::TreeReconstruct)?;
            tracing::trace!("Reconstructed {i} nodes with {} bytes total", n.size());

            Ok(Item::new(name, path, revision, description, &n))
        }
    }

    /// Minimal set of hashes required to reconstruct `target` using `from`
    ///
    /// # Errors
    /// Returns None if `target` doesn't exist in storage
    fn diff(&self, target: &Hash, from: &[Hash]) -> Option<HashSet<Hash>> {
        let target_chunk = self.get(target)?;
        from.iter()
            .filter_map(|from_hash| self.get(from_hash))
            .fold(target_chunk.hashes(), |t: HashSet<Hash>, from_chunk| {
                t.difference(&from_chunk.hashes()).copied().collect()
            })
            .into()
    }

    /// Take ownership of an `OwnedHashTreeNode` and try to fill in any `Skipped` nodes
    fn try_fill_in(&mut self, tree: &Node) -> Option<Arc<Node>> {
        tracing::trace!("Filling {}", tree.hash());
        Some(match tree {
            Node::Stored { hash, data } => self._insert_chunk(*hash, &data)?,
            Node::Parent { left, right, .. } => {
                let l = self.try_fill_in(left)?;
                let r = self.try_fill_in(right)?;
                self.link(l, r)?
            }
            Node::Skipped { hash, .. } => self.get(&hash)?,
        })
    }
}
