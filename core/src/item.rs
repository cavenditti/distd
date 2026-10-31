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

use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
//use ring::signature::Signature;

use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

use crate::chunk_storage::Node;
use crate::chunks::{ChunkAlgorithm, ChunkInfo};
use crate::hash::Hash as Blake3Hash;
use crate::metadata::Item as ItemMetadata;
use crate::unique_name::UniqueName;

pub type Name = UniqueName;

/// Stable artifact identity — persists across revisions and path changes.
pub type ArtifactId = String;

/// Maximum byte length of a symbolic-link target (`PATH_MAX - 1`).
pub const MAX_SYMLINK_TARGET_BYTES: usize = 4095;

/// Filesystem object kind of a manifest entry.
///
/// Every kind keeps the chunk-range invariants of a regular file: a symbolic link is a size-0
/// entry covering exactly one empty chunk, and its target is carried only by the entry itself.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Default)]
pub enum EntryKind {
    /// Read-only regular file (`0o444`).
    #[default]
    Regular,
    /// Read-only executable regular file (`0o555`).
    Executable,
    /// Symbolic link to a publisher-signed target, which may be absolute (for example a Nix
    /// store path). Targets are never followed while storing or activating a revision.
    Symlink { target: String },
}

impl EntryKind {
    #[must_use]
    pub const fn is_regular(&self) -> bool {
        matches!(self, Self::Regular)
    }

    #[must_use]
    pub fn symlink_target(&self) -> Option<&str> {
        match self {
            Self::Symlink { target } => Some(target),
            Self::Regular | Self::Executable => None,
        }
    }

    /// POSIX permission bits this kind materializes with.
    #[must_use]
    pub const fn permissions(&self) -> u16 {
        match self {
            Self::Regular => 0o444,
            Self::Executable => 0o555,
            Self::Symlink { .. } => 0o777,
        }
    }

    /// Check the kind's own invariants: a symbolic-link target is nonempty, at most
    /// [`MAX_SYMLINK_TARGET_BYTES`] long, free of NUL bytes, and NFC-normalized UTF-8.
    pub fn validate(&self) -> Result<(), &'static str> {
        let Some(target) = self.symlink_target() else {
            return Ok(());
        };
        if target.is_empty() {
            return Err("symlink target is empty");
        }
        if target.len() > MAX_SYMLINK_TARGET_BYTES {
            return Err("symlink target exceeds its length limit");
        }
        if target.as_bytes().contains(&0) {
            return Err("symlink target contains a NUL byte");
        }
        if !target.is_ascii() && !target.nfc().eq(target.chars()) {
            return Err("symlink target is not NFC");
        }
        Ok(())
    }
}

/// A single file within a multi-file artifact.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Relative path within the artifact
    pub relative_path: String,
    /// File size in bytes (`0` for symbolic links)
    pub size: u64,
    /// Range of global chunk indices [start, end) that cover this file
    pub chunk_range: (u32, u32),
    /// Filesystem object kind
    #[serde(default)]
    pub kind: EntryKind,
}

impl FileEntry {
    /// Wire form of this entry; the kind is omitted for regular files.
    #[must_use]
    pub fn to_proto(&self) -> crate::proto::FileEntry {
        let (kind, symlink_target) = match &self.kind {
            EntryKind::Regular => (None, None),
            EntryKind::Executable => (Some(crate::proto::EntryKind::Executable as i32), None),
            EntryKind::Symlink { target } => (
                Some(crate::proto::EntryKind::Symlink as i32),
                Some(target.clone()),
            ),
        };
        crate::proto::FileEntry {
            relative_path: self.relative_path.clone(),
            size: self.size,
            chunk_start: self.chunk_range.0,
            chunk_end: self.chunk_range.1,
            kind,
            symlink_target,
        }
    }

    /// Decode a wire entry, rejecting unknown kinds and targets that do not belong to a
    /// symbolic link. Range, size, and path validity are checked with the whole manifest.
    pub fn try_from_proto(entry: crate::proto::FileEntry) -> Result<Self, &'static str> {
        use crate::proto::EntryKind as ProtoKind;
        let kind = match (entry.kind.map(ProtoKind::try_from), entry.symlink_target) {
            (None | Some(Ok(ProtoKind::Regular)), None) => EntryKind::Regular,
            (Some(Ok(ProtoKind::Executable)), None) => EntryKind::Executable,
            (Some(Ok(ProtoKind::Symlink)), Some(target)) => EntryKind::Symlink { target },
            (Some(Ok(ProtoKind::Symlink)), None) => return Err("symlink entry has no target"),
            (_, Some(_)) => return Err("non-symlink entry has a symlink target"),
            (Some(Ok(ProtoKind::Unspecified) | Err(_)), None) => {
                return Err("entry kind is unspecified or unknown")
            }
        };
        Ok(Self {
            relative_path: entry.relative_path,
            size: entry.size,
            chunk_range: (entry.chunk_start, entry.chunk_end),
            kind,
        })
    }

    /// A regular-file entry.
    #[must_use]
    pub fn regular(relative_path: String, size: u64, chunk_range: (u32, u32)) -> Self {
        Self {
            relative_path,
            size,
            chunk_range,
            kind: EntryKind::Regular,
        }
    }
}

/// Compact manifest describing an artifact — no full chunk list.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub artifact_id: ArtifactId,
    pub version: u64,
    pub root_hash: Blake3Hash,
    pub total_size: u64,
    pub chunk_count: u32,
    pub chunk_size: u32, // nominal (256 KiB), last chunk smaller
    /// Chunking algorithm used (default: Fixed256K)
    #[serde(default)]
    pub chunk_algorithm: ChunkAlgorithm,
    /// File entries for multi-file artifacts. Empty for single-file artifacts.
    #[serde(default)]
    pub entries: Vec<FileEntry>,
}

impl Manifest {
    /// True when every entry is a regular file. Such manifests keep the exact signed and
    /// persisted encodings they had before entry kinds existed.
    #[must_use]
    pub fn has_only_regular_entries(&self) -> bool {
        self.entries.iter().all(|entry| entry.kind.is_regular())
    }

    /// Build a manifest from a hash tree root node and chunk list.
    #[must_use]
    pub fn from_tree(
        artifact_id: ArtifactId,
        version: u64,
        root: &Arc<Node>,
        chunks: &[ChunkInfo],
        chunk_algorithm: ChunkAlgorithm,
    ) -> Self {
        let total_size = root.size();
        let chunk_count = if chunks.is_empty() {
            1
        } else {
            chunks.len() as u32
        };
        Self {
            artifact_id,
            version,
            root_hash: *root.hash(),
            total_size,
            chunk_count,
            chunk_size: chunk_algorithm.nominal_chunk_size(),
            chunk_algorithm,
            entries: Vec::new(),
        }
    }
}

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
    /// Compact manifest (always available, cheap to compute)
    pub manifest: Manifest,
    /// BLAKE3 hashes of the chunks that make the item
    pub chunks: Vec<ChunkInfo>,
}

impl Item {
    #[must_use]
    pub fn with_artifact_id(mut self, artifact_id: ArtifactId) -> Self {
        self.metadata.artifact_id.clone_from(&artifact_id);
        self.manifest.artifact_id = artifact_id;
        self
    }

    /// Bind a side-effect-free prepared item to the revision selected by the
    /// publication coordinator.
    #[must_use]
    pub fn with_revision(mut self, revision: u32) -> Self {
        self.metadata.revision = revision;
        self.manifest.version = u64::from(revision);
        self
    }

    /// Create a new Item from its metadata and `Node`
    ///
    /// Calling `create_item` on a `ChunkStorage` object encapsulates this and its the recommended way to create
    /// an Item unless there is an explicit reason not to do so.
    ///
    /// `artifact_id` is derived from the `name` if not provided explicitly.
    #[must_use]
    pub fn new(
        name: Name,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        hash_tree: &Arc<Node>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Self {
        let artifact_id = name.clone();
        let chunks = hash_tree.flatten_with_sizes().unwrap_or_default();
        let manifest = Manifest::from_tree(
            artifact_id.clone(),
            revision as u64,
            hash_tree,
            &chunks,
            chunk_algorithm,
        );
        let now = SystemTime::now();
        Self {
            metadata: ItemMetadata {
                artifact_id,
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
            manifest,
            chunks,
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
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Self, std::io::Error> {
        Self::make_with_entries(
            name,
            path,
            revision,
            description,
            root,
            chunks,
            Vec::new(),
            chunk_algorithm,
        )
    }

    pub fn make_with_entries(
        name: Name,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        root: ChunkInfo,
        chunks: Vec<ChunkInfo>,
        entries: Vec<FileEntry>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Self, std::io::Error> {
        let artifact_id = name.clone();
        let chunk_count = if chunks.is_empty() {
            1
        } else {
            chunks.len() as u32
        };
        let manifest = Manifest {
            artifact_id: artifact_id.clone(),
            version: revision as u64,
            root_hash: root.hash,
            total_size: root.size,
            chunk_count,
            chunk_size: chunk_algorithm.nominal_chunk_size(),
            chunk_algorithm,
            entries,
        };
        let now = SystemTime::now();
        Ok(Self {
            metadata: ItemMetadata {
                artifact_id,
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
            manifest,
            chunks,
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
    pub fn diff(&self, other: &Self) -> std::collections::HashSet<ChunkInfo> {
        let own_chunks = self
            .chunks
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let other_chunks = other
            .chunks
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        own_chunks.difference(&other_chunks).copied().collect()
    }

    #[inline]
    #[must_use]
    pub fn root(&self) -> &Blake3Hash {
        &self.metadata.root.hash
    }
}

/// Frozen persisted layouts from before [`EntryKind`] existed.
///
/// Bitcode is not self-describing, so stored revisions must be decoded with the exact field
/// layout they were written with. Regular-only revisions are still written in this layout, which
/// keeps their persisted bytes and every identity derived from them unchanged.
pub mod v1 {
    use serde::{Deserialize, Serialize};

    use super::{ArtifactId, EntryKind, FileEntry, Item, Manifest};
    use crate::chunks::{ChunkAlgorithm, ChunkInfo};
    use crate::hash::Hash as Blake3Hash;
    use crate::metadata::Item as ItemMetadata;

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub struct FileEntryV1 {
        pub relative_path: String,
        pub size: u64,
        pub chunk_range: (u32, u32),
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub struct ManifestV1 {
        pub artifact_id: ArtifactId,
        pub version: u64,
        pub root_hash: Blake3Hash,
        pub total_size: u64,
        pub chunk_count: u32,
        pub chunk_size: u32,
        #[serde(default)]
        pub chunk_algorithm: ChunkAlgorithm,
        #[serde(default)]
        pub entries: Vec<FileEntryV1>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub struct ItemV1 {
        pub metadata: ItemMetadata,
        pub manifest: ManifestV1,
        pub chunks: Vec<ChunkInfo>,
    }

    impl std::hash::Hash for ItemV1 {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.metadata.hash(state);
        }
    }

    impl ItemV1 {
        /// The V1 layout of `item`, or `None` when it has an entry the layout cannot express.
        #[must_use]
        pub fn from_regular(item: &Item) -> Option<Self> {
            if !item.manifest.has_only_regular_entries() {
                return None;
            }
            let manifest = &item.manifest;
            Some(Self {
                metadata: item.metadata.clone(),
                manifest: ManifestV1 {
                    artifact_id: manifest.artifact_id.clone(),
                    version: manifest.version,
                    root_hash: manifest.root_hash,
                    total_size: manifest.total_size,
                    chunk_count: manifest.chunk_count,
                    chunk_size: manifest.chunk_size,
                    chunk_algorithm: manifest.chunk_algorithm,
                    entries: manifest
                        .entries
                        .iter()
                        .map(|entry| FileEntryV1 {
                            relative_path: entry.relative_path.clone(),
                            size: entry.size,
                            chunk_range: entry.chunk_range,
                        })
                        .collect(),
                },
                chunks: item.chunks.clone(),
            })
        }
    }

    const KINDS_UNSUPPORTED: &str = "the V1 revision layout cannot encode entry kinds";

    /// Serializes an [`Item`] in the V1 layout, failing for revisions with entry kinds.
    struct Layout<'a>(&'a Item);

    impl Serialize for Layout<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            ItemV1::from_regular(self.0)
                .ok_or_else(|| serde::ser::Error::custom(KINDS_UNSUPPORTED))?
                .serialize(serializer)
        }
    }

    /// `#[serde(with)]` adapter storing one [`Item`] in the V1 layout.
    pub mod item {
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        use super::{ItemV1, Layout};
        use crate::item::Item;

        pub fn serialize<S: Serializer>(item: &Item, serializer: S) -> Result<S::Ok, S::Error> {
            Layout(item).serialize(serializer)
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Item, D::Error> {
            ItemV1::deserialize(deserializer).map(Item::from)
        }
    }

    /// `#[serde(with)]` adapter storing a sequence or set of [`Item`]s in the V1 layout.
    pub mod seq {
        use serde::{Deserialize, Deserializer, Serializer};

        use super::{ItemV1, Layout};
        use crate::item::Item;

        pub fn serialize<'a, C, S>(items: &'a C, serializer: S) -> Result<S::Ok, S::Error>
        where
            &'a C: IntoIterator<Item = &'a Item>,
            S: Serializer,
        {
            serializer.collect_seq(items.into_iter().map(Layout))
        }

        pub fn deserialize<'de, C, D>(deserializer: D) -> Result<C, D::Error>
        where
            C: FromIterator<Item>,
            D: Deserializer<'de>,
        {
            Ok(Vec::<ItemV1>::deserialize(deserializer)?
                .into_iter()
                .map(Item::from)
                .collect())
        }
    }

    /// `#[serde(with)]` adapter storing a map with [`Item`] values in the V1 layout.
    pub mod map {
        use std::marker::PhantomData;

        use serde::de::{MapAccess, Visitor};
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        use super::{ItemV1, Layout};
        use crate::item::Item;

        pub fn serialize<'a, K, C, S>(items: &'a C, serializer: S) -> Result<S::Ok, S::Error>
        where
            K: Serialize + 'a,
            &'a C: IntoIterator<Item = (&'a K, &'a Item)>,
            S: Serializer,
        {
            serializer.collect_map(items.into_iter().map(|(key, item)| (key, Layout(item))))
        }

        pub fn deserialize<'de, K, C, D>(deserializer: D) -> Result<C, D::Error>
        where
            K: Deserialize<'de>,
            C: FromIterator<(K, Item)>,
            D: Deserializer<'de>,
        {
            struct PairsVisitor<K>(PhantomData<K>);

            impl<'de, K: Deserialize<'de>> Visitor<'de> for PairsVisitor<K> {
                type Value = Vec<(K, ItemV1)>;

                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("a map of V1 revisions")
                }

                fn visit_map<A: MapAccess<'de>>(
                    self,
                    mut access: A,
                ) -> Result<Self::Value, A::Error> {
                    let mut pairs = Vec::with_capacity(access.size_hint().unwrap_or(0).min(4096));
                    while let Some(pair) = access.next_entry()? {
                        pairs.push(pair);
                    }
                    Ok(pairs)
                }
            }

            Ok(deserializer
                .deserialize_map(PairsVisitor(PhantomData))?
                .into_iter()
                .map(|(key, item)| (key, Item::from(item)))
                .collect())
        }
    }

    impl From<ItemV1> for Item {
        fn from(value: ItemV1) -> Self {
            let manifest = value.manifest;
            Self {
                metadata: value.metadata,
                manifest: Manifest {
                    artifact_id: manifest.artifact_id,
                    version: manifest.version,
                    root_hash: manifest.root_hash,
                    total_size: manifest.total_size,
                    chunk_count: manifest.chunk_count,
                    chunk_size: manifest.chunk_size,
                    chunk_algorithm: manifest.chunk_algorithm,
                    entries: manifest
                        .entries
                        .into_iter()
                        .map(|entry| FileEntry {
                            relative_path: entry.relative_path,
                            size: entry.size,
                            chunk_range: entry.chunk_range,
                            kind: EntryKind::Regular,
                        })
                        .collect(),
                },
                chunks: value.chunks,
            }
        }
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

    use bytes::Bytes;

    use crate::chunk_storage::hashmap_storage::HashMapStorage;
    use crate::chunk_storage::ChunkStorage;
    use crate::chunks::CHUNK_SIZE;
    use crate::error::Error;
    use crate::hash::hash;
    use crate::utils::serde::BitcodeSerializable;
    use crate::utils::testing::random_path_subdir;

    use super::*;

    /*
     * Most of these tests are bad. I wasn't sure about Item interfaces at first and their spaghettified
     * and messed up
     */

    pub fn new_empty_item<T>(storage: &T) -> Result<Item, Error>
    where
        T: ChunkStorage,
    {
        storage.create_item(
            "name".to_string(),
            random_path_subdir(),
            0,
            None,
            Bytes::from_static(b""),
            ChunkAlgorithm::default(),
        )
    }

    pub fn new_dummy_item<T, const VALUE: u8, const SIZE: usize>(storage: &T) -> Result<Item, Error>
    where
        T: ChunkStorage,
    {
        println!("Inserting {SIZE} {VALUE}u8");

        storage.create_item(
            "name".to_string(),
            random_path_subdir(),
            0,
            Some("Some description for the larger item".to_string()),
            Bytes::from_static(&[VALUE; SIZE]),
            ChunkAlgorithm::default(),
        )
    }

    pub fn new_zeros_item<T>(storage: &T) -> Result<Item, Error>
    where
        T: ChunkStorage,
    {
        new_dummy_item::<T, 0u8, 100_000_000>(storage)
    }

    pub fn new_ones_item<T>(storage: &T) -> Result<Item, Error>
    where
        T: ChunkStorage,
    {
        new_dummy_item::<T, 1u8, 100_000_000>(storage)
    }

    #[must_use]
    pub fn make_repeated_item(value: u8) -> Option<Item> {
        let data = Bytes::from_iter([value; CHUNK_SIZE]);
        let chunk = ChunkInfo {
            hash: hash(&data),
            size: CHUNK_SIZE as u64,
        };
        Item::make(
            "name".to_string(),
            random_path_subdir(),
            0,
            Some("Some description for the larger item".to_string()),
            chunk,
            vec![chunk],
            ChunkAlgorithm::default(),
        )
        .ok()
    }

    #[must_use]
    pub fn make_zeros_item() -> Option<Item> {
        make_repeated_item(0)
    }

    #[must_use]
    pub fn make_ones_item() -> Option<Item> {
        make_repeated_item(1)
    }

    #[test]
    fn make_item() {
        let _ = make_zeros_item();
    }

    #[test]
    fn item_size() {
        println!("In-memory size of Item:         {}", mem::size_of::<Item>());
        println!(
            "In-memory size of ItemMetadata: {}",
            mem::size_of::<ItemMetadata>()
        );
        {
            let mut storage = HashMapStorage::default();
            let item = new_empty_item(&mut storage).unwrap();
            let serialized = item.clone().metadata.to_bitcode().unwrap();
            println!("Small Item serialized size: {}", serialized.len());

            let new_metadata = ItemMetadata::from_bitcode(&serialized).unwrap();
            assert_eq!(item.metadata, new_metadata);
        }

        {
            // Same as above but with a larger one
            let mut storage = HashMapStorage::default();
            let item = new_zeros_item(&mut storage).unwrap();
            let serialized = item.clone().metadata.to_bitcode().unwrap();
            println!("Small Item serialized size: {}", serialized.len());

            let new_metadata = ItemMetadata::from_bitcode(&serialized).unwrap();
            assert_eq!(item.metadata, new_metadata);
        }
    }
}
