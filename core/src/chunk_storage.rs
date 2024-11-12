use std::{collections::HashSet, path::PathBuf, sync::Arc};

use bytes::Bytes;
pub use node::Node;
use tokio_stream::{Stream, StreamExt};

use crate::error::Error;
use crate::hash::{hash, Hash, HashTreeCapable};
use crate::{
    hash::merge_hashes,
    item::{Item, Name as ItemName},
};

pub mod fs_storage;
pub mod hashmap_storage;
pub mod node;
pub mod node_stream;

#[cfg(feature = "redb")]
pub mod redb;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Unknown storage size")]
    UnknownSize,

    #[error("Cannot insert chunk in data store")]
    ChunkInsertError,

    #[error("Cannot insert chunk in data store")]
    UnknownChunkInsertError(#[from] std::io::Error),

    #[error("Cannot create link")]
    LinkCreation,

    #[error("Cannot reconstruct tree from storage")]
    TreeReconstruct,
}

/// Defines a backend used to store hashes and chunks ad key-value pairs
pub trait ChunkStorage: HashTreeCapable<Arc<Node>, Error> {
    fn get(&self, hash: &Hash) -> Option<Arc<Node>>;
    fn _insert_chunk(&mut self, hash: Hash, chunk: &[u8]) -> Option<Arc<Node>>;
    fn _link(&mut self, hash: Hash, left: Arc<Node>, right: Arc<Node>) -> Option<Arc<Node>>;

    fn chunks(&self) -> Vec<Hash>;

    /// Allocated size for all chunks, in bytes
    /// This only counts actual chunks size, excluding any auxiliary structure used by storage backend/adapter
    fn size(&self) -> u64;

    //fn drop(hash: Hash); // TODO

    fn insert_chunk(&mut self, chunk: &[u8]) -> Option<Arc<Node>> {
        let hash = hash(chunk);
        tracing::trace!("Insert chunk {hash}, {} bytes", chunk.len());

        self._insert_chunk(hash, chunk)
            .inspect(|x| assert!(x.hash() == &hash))
    }

    fn link(&mut self, left: Arc<Node>, right: Arc<Node>) -> Option<Arc<Node>> {
        let hash = merge_hashes(left.hash(), right.hash());
        tracing::trace!("Link {} {} → {}", left.hash(), right.hash(), hash);
        self._link(hash, left, right)
            .inspect(|x| assert!(x.hash() == &hash))
    }

    /// Insert bytes into the storage returning the associated hash tree
    fn insert(&mut self, data: Bytes) -> Option<Arc<Node>>
    where
        Self: Sized,
    {
        self.compute_tree(data.as_ref()).ok()
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
        T: Stream<Item = Node> + std::marker::Unpin + Send,
    {
        async move {
            let mut n = None; // final node
            let mut i = 0; // node counter
            while let Some(node) = stream.next().await {
                n = Some(
                    self.try_fill_in(&node)
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
            Node::Stored { hash, data } => self._insert_chunk(*hash, data)?,
            Node::Parent { left, right, .. } => {
                let l = self.try_fill_in(left)?;
                let r = self.try_fill_in(right)?;
                self.link(l, r)?
            }
            Node::Skipped { hash, .. } => self.get(hash)?,
        })
    }
}

/// Tests for `ChunkStorage` implementations
///
/// The `chunk_storage_tests` macro generates tests for a `ChunkStorage` implementation.
/// All the tests are run with a clean storage instance, provided by the `builder` function.
#[cfg(test)]
mod tests {
    use super::*;

    use bytes::{Bytes, BytesMut};
    use rand::{self, RngCore};

    use crate::{chunks::CHUNK_SIZE, hash::hash};

    pub fn single_chunk_insertion<S>(s: &mut S)
    where
        S: ChunkStorage,
    {
        let data = Bytes::from_static(b"very few bytes");
        let len = data.len() as u64;
        s.insert(data);
        assert_eq!(len, s.size());
    }

    /// Multiple chunks, not aligned with `CHUNK_SIZE`
    pub fn multiple_chunks_insertion<S>(s: &mut S)
    where
        S: ChunkStorage,
    {
        let data = Bytes::from_static(include_bytes!("../../Cargo.lock"));
        let len = data.len() as u64;
        //let root = s.insert(data).unwrap();
        println!("\nOriginal lenght: {}, stored length: {}", len, s.size());
        //print_tree(&*root.to_owned()).unwrap();
        println!();
        assert!(len >= s.size());
    }

    pub fn chunks_deduplication<S>(s: &mut S)
    where
        S: ChunkStorage,
    {
        const MULT: usize = 3;
        const SIZE: usize = CHUNK_SIZE * MULT;
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

    pub fn storage_2mb<S>(s: &mut S)
    where
        S: ChunkStorage,
    {
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

    macro_rules! chunk_storage_tests {
        ($t:ty, $builder:ident) => {
            crate::chunk_storage::tests::chunk_storage_tests!($t, single_chunk_insertion, $builder);
            crate::chunk_storage::tests::chunk_storage_tests!($t, multiple_chunks_insertion, $builder);
            crate::chunk_storage::tests::chunk_storage_tests!($t, chunks_deduplication, $builder);
            crate::chunk_storage::tests::chunk_storage_tests!($t, storage_2mb, $builder);
            // ... any more tests go here ...
        };
        ($t:ty, $name:ident, $builder:ident) => {
            #[test]
            fn $name() {
                $crate::chunk_storage::tests::$name::<$t>(&mut $builder());
            }
        };
    }

    pub(crate) use chunk_storage_tests;
}
