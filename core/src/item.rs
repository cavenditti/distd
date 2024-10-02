//!distd metadata format
//!
//!This is the reference strucure in JSON.
//!This is very similar to a torrent/metainfo file. Some differences:
//! - 1. We're ignoring everything tracker related, as we already know the server and it does most of what of tracker
//!         shoud do
//! - 2. Uses BLAKE3 instead of SHA1 or SHA25
//! - 3. No `info_hash`, no encoding. We'll use a binary format and the server will sign it
//! - 4. We're calling them "chunks" instead of "pieces", because I like it more this way
//! - 5. An item contains a single file. Msgpack serialization is cheap. Just use tar if you need to :)
//!
//!```json
//!"name": "Update for some file",
//!"description": "Description field, a string to put whatever you like",
//!"path": "relative/path/for/file.ext",
//!"revision": "2"
//!"created": 1375363666,
//!"created_by": "distd 0.1.0",
//!"format": "1"
//!"chunk_size": 16384,
//!"chunks": [
//!  "8a468d4f30b20645981364d3b77499f0d3dc999d25960cdfc5da8e836ce51b9d",
//!  "36875eae0dba363968a1e2f12d6be4aff5d737d0cca2d12351ccf182531a8613",
//!  ...
//! ]
//!"signature": <build-key signature of file> ???
//!}
//!```

use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
//use ring::signature::Signature;

use serde::{Deserialize, Serialize};

use crate::chunk_storage::Node;
use crate::chunks::ChunkInfo;
use crate::metadata::Item as ItemMetadata;
use crate::unique_name::UniqueName;

pub type Name = UniqueName;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    V1 = 1,
}

/// Item representation
///
/// This is bothe the format used over-the-wire to communicate from client to server, as well as the internal format
/// used by both server and client/peers.
///
/// We're assuming this is produced by a non-ill-intended trusted party, and we're not permorming many checks (e.g. on
/// name and descprition length).
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Item {
    /// General metadata of the Item
    pub metadata: ItemMetadata,
    /// BLAKE3 hashes of the chunks that make the item
    pub chunks: Vec<ChunkInfo>,
    /// BLAKE3 hashes of any hash subtree
    pub hashes: HashSet<ChunkInfo>,
}

impl Item {
    /// Create a new Item from its metadata and `Node`
    ///
    /// Calling `create_item` on a `ChunkStorage` object encapsulates this and its the recommended way to create
    /// an Item unless there is an explicit reason not to do so.
    #[must_use]
    pub fn new(
        name: Name,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        hash_tree: &Arc<Node>,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            metadata: ItemMetadata {
                name,
                description,
                revision,
                path,
                root: hash_tree.chunk_info(),
                created: now,
                updated: now,
                created_by: env!("CARGO_PKG_VERSION").to_owned(),
                format: Format::V1,
            },
            chunks: hash_tree.flatten_with_sizes(),
            hashes: hash_tree.all_hashes_with_sizes(),
        }
    }

    /// Make a new Item without adding it to a storage
    pub fn make(
        name: Name,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        root: ChunkInfo,
        chunks: Vec<ChunkInfo>,
        hashes: HashSet<ChunkInfo>,
    ) -> Result<Self, std::io::Error> {
        let now = SystemTime::now();
        Ok(Self {
            metadata: ItemMetadata {
                name,
                description,
                revision,
                path,
                root,
                created: now,
                updated: now,
                created_by: env!("CARGO_PKG_VERSION").to_owned(),
                format: Format::V1,
            },
            chunks,
            hashes,
        })
    }

    /// Recompute total size of the item
    /// Computed as the sum of the sizes of the chunks
    #[must_use]
    pub fn recompute_size(&self) -> u64 {
        // useful?
        self.chunks.iter().map(|x| x.size).sum()
    }

    /// Total size of the item
    #[must_use]
    pub fn size(&self) -> u64 {
        self.metadata.size()
    }

    /// `Stored` chunks diff of two items
    /// Chunks in self and not in other
    #[must_use]
    pub fn diff(&self, other: &Self) -> HashSet<ChunkInfo> {
        self.hashes.difference(&other.hashes).copied().collect()
    }

    #[inline(always)]
    pub fn root(&self) -> &crate::hash::Hash {
        &self.metadata.root.hash
    }
}

impl std::hash::Hash for Item {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.metadata.hash(state);
    }
}

impl Display for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut desc = self.metadata.description.clone().unwrap_or_default();
        if !desc.is_empty() {
            desc = format!("description: {desc}, ");
        }
        write!(
            f,
            "Item {{ {}, revision: {}, name: {}, {desc}root: {}, {}B in {} chunks }}",
            self.metadata.path.to_string_lossy(),
            self.metadata.revision,
            self.metadata.name,
            self.metadata.root.hash,
            self.metadata.root.size,
            self.chunks.len(),
        )
    }
}

#[cfg(test)]
pub mod tests {
    use std::mem;
    use std::str::FromStr;

    use bytes::Bytes;
    use uuid::Uuid;

    use crate::chunk_storage::hashmap_storage::HashMapStorage;
    use crate::chunk_storage::ChunkStorage;
    use crate::chunks::CHUNK_SIZE;
    use crate::hash::hash;
    use crate::utils::serde::BitcodeSerializable;

    use super::*;

    // Using a random path to avoid confliting file creation from mutliple concurrent tests
    #[must_use]
    pub fn random_path() -> PathBuf {
        PathBuf::from_str(&format!("random/unique/path/{}", Uuid::new_v4())).unwrap()
    }

    /*
     * Most of these tests are bad. I wasn't sure about Item interfaces at first and their spaghettified
     * and messed up
     */

    pub fn new_empty_item<T>(storage: T) -> Item
    where
        T: ChunkStorage + Clone,
    {
        storage
            .create_item(
                "name".to_string(),
                random_path(),
                0,
                None,
                Bytes::from_static(b""),
            )
            .unwrap()
    }

    pub fn new_dummy_item<T, const VALUE: u8, const SIZE: usize>(storage: &T) -> Item
    where
        T: ChunkStorage,
    {
        println!("Inserting {SIZE} {VALUE}u8");

        storage
            .create_item(
                "name".to_string(),
                random_path(),
                0,
                Some("Some description for the larger item".to_string()),
                Bytes::from_static(&[VALUE; SIZE]),
            )
            .unwrap()
    }

    pub fn new_zeros_item<T>(storage: &T) -> Item
    where
        T: ChunkStorage + Clone,
    {
        new_dummy_item::<T, 0u8, 100_000_000>(storage)
    }

    pub fn new_ones_item<T>(storage: &T) -> Item
    where
        T: ChunkStorage + Clone,
    {
        new_dummy_item::<T, 1u8, 100_000_000>(storage)
    }

    #[must_use]
    pub fn make_repeated_item(value: u8) -> Item {
        let data = Bytes::from_iter([value; CHUNK_SIZE]);
        let chunk = ChunkInfo {
            hash: hash(&data),
            size: CHUNK_SIZE as u64,
        };
        Item::make(
            "name".to_string(),
            random_path(),
            0,
            Some("Some description for the larger item".to_string()),
            chunk,
            vec![chunk],
            HashSet::from_iter(vec![chunk]),
        )
        .unwrap()
    }

    #[must_use]
    pub fn make_zeros_item() -> Item {
        make_repeated_item(0)
    }

    #[must_use]
    pub fn make_ones_item() -> Item {
        make_repeated_item(1)
    }

    #[test]
    fn test_make_item() {
        let _ = make_zeros_item();
    }

    #[test]
    fn test_item_size() {
        println!("In-memory size of Item:         {}", mem::size_of::<Item>());
        println!(
            "In-memory size of ItemMetadata: {}",
            mem::size_of::<ItemMetadata>()
        );
        {
            let storage = HashMapStorage::default();
            let item = new_empty_item(storage);
            let serialized = item.clone().metadata.to_bitcode().unwrap();
            println!("Small Item serialized size: {}", serialized.len());

            let new_metadata = ItemMetadata::from_bitcode(&serialized).unwrap();
            assert_eq!(item.metadata, new_metadata);
        }

        {
            // Same as above but with a larger one
            let storage = HashMapStorage::default();
            let item = new_zeros_item(&storage);
            let serialized = item.clone().metadata.to_bitcode().unwrap();
            println!("Small Item serialized size: {}", serialized.len());

            let new_metadata = ItemMetadata::from_bitcode(&serialized).unwrap();
            assert_eq!(item.metadata, new_metadata);
        }
    }
}
