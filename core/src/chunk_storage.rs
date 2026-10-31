use std::{
    collections::{BTreeMap, HashSet},
    ops::{Deref, Range},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::Bytes;
pub use node::Node;
use tokio_stream::{Stream, StreamExt};

use crate::error::Error;
use crate::hash::{hash, Hash, HashTreeCapable};
use crate::{
    chunks::{ChunkAlgorithm, ChunkInfo},
    hash::merge_hashes,
    item::{ArtifactId, EntryKind, FileEntry, Item, Name as ItemName},
};

static NEXT_RECEIVED_BACKING_ID: AtomicU64 = AtomicU64::new(1);

fn next_received_backing_id() -> u64 {
    next_received_backing_id_from(&NEXT_RECEIVED_BACKING_ID)
}

fn next_received_backing_id_from(counter: &AtomicU64) -> u64 {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .unwrap_or_else(|_| panic!("received chunk backing identity space exhausted"))
}

fn prepared_tree(chunks: &[ChunkInfo]) -> Result<ChunkInfo, Error> {
    let mut partials = chunks.to_vec();
    if partials.is_empty() {
        return Err(Error::MissingData);
    }
    while partials.len() > 1 {
        let n = partials.len();
        for (to, index) in (0..n - 1).step_by(2).enumerate() {
            partials[to] = ChunkInfo {
                hash: merge_hashes(&partials[index].hash, &partials[index + 1].hash),
                size: partials[index]
                    .size
                    .checked_add(partials[index + 1].size)
                    .ok_or_else(|| Error::Other("revision size overflows u64".to_string()))?,
            };
        }
        let half = n / 2;
        if n % 2 != 0 {
            partials.swap(half, n - 1);
            partials.truncate(half + 1);
        } else {
            partials.truncate(half);
        }
    }
    Ok(partials.swap_remove(0))
}

fn prepared_chunks(data: &[u8], chunk_algorithm: ChunkAlgorithm) -> Vec<ChunkInfo> {
    if data.is_empty() {
        return vec![ChunkInfo {
            hash: hash(&[]),
            size: 0,
        }];
    }
    chunk_algorithm
        .chunk_boundaries(data)
        .into_iter()
        .map(|(offset, length)| ChunkInfo {
            hash: hash(&data[offset..offset + length]),
            size: length as u64,
        })
        .collect()
}

fn same_revision(candidate: &Item, actual: &Item) -> bool {
    candidate.metadata.artifact_id == actual.metadata.artifact_id
        && candidate.metadata.name == actual.metadata.name
        && candidate.metadata.description == actual.metadata.description
        && candidate.metadata.revision == actual.metadata.revision
        && candidate.metadata.path == actual.metadata.path
        && candidate.metadata.root == actual.metadata.root
        && candidate.manifest == actual.manifest
        && candidate.chunks == actual.chunks
}

/// Non-regular entry kinds of `item`, keyed by relative path, in the form the directory
/// preparation API accepts.
pub(crate) fn entry_kinds(item: &Item) -> BTreeMap<PathBuf, EntryKind> {
    item.manifest
        .entries
        .iter()
        .filter(|entry| !entry.kind.is_regular())
        .map(|entry| (PathBuf::from(&entry.relative_path), entry.kind.clone()))
        .collect()
}

fn invalid_committed_revision(reason: impl Into<String>) -> Error {
    Error::Other(format!("invalid committed revision: {}", reason.into()))
}

/// Validate the storage-independent identity and tree semantics of a published revision.
///
/// This does not prove that the declared chunks are present in a selected backend. Backends use
/// the same validation before checking their exact retained bytes.
pub fn validate_revision_metadata(item: &Item) -> Result<ChunkInfo, Error> {
    validate_revision_authority(&item.metadata.artifact_id, &item.metadata.path)?;
    if item.metadata.artifact_id != item.manifest.artifact_id {
        return Err(invalid_committed_revision(
            "metadata and manifest artifact IDs differ",
        ));
    }
    if u64::from(item.metadata.revision) != item.manifest.version {
        return Err(invalid_committed_revision(
            "metadata and manifest revisions differ",
        ));
    }
    if item.chunks.is_empty() {
        return Err(invalid_committed_revision("chunk list is empty"));
    }
    if usize::try_from(item.manifest.chunk_count).ok() != Some(item.chunks.len()) {
        return Err(invalid_committed_revision(
            "manifest chunk count differs from chunk list",
        ));
    }
    if item.manifest.chunk_size != item.manifest.chunk_algorithm.nominal_chunk_size() {
        return Err(invalid_committed_revision(
            "manifest chunk size differs from its algorithm",
        ));
    }

    let root = prepared_tree(&item.chunks)?;
    if root != item.metadata.root
        || root.hash != item.manifest.root_hash
        || root.size != item.manifest.total_size
    {
        return Err(invalid_committed_revision(
            "declared chunks do not reconstruct the declared root",
        ));
    }

    let mut next_chunk = 0usize;
    let mut previous_path: Option<PathBuf> = None;
    for entry in &item.manifest.entries {
        let path = PathBuf::from(&entry.relative_path);
        let canonical = crate::utils::path::validate_relative(&path)?;
        if canonical != path || canonical.to_str() != Some(entry.relative_path.as_str()) {
            return Err(invalid_committed_revision(
                "directory entry path is not canonical UTF-8",
            ));
        }
        if canonical.components().next().is_some_and(|component| {
            component.as_os_str() == std::ffi::OsStr::new(".distd-revisions-v1")
                || component.as_os_str() == std::ffi::OsStr::new(".distd-activation-v1")
        }) {
            return Err(invalid_committed_revision(
                "directory entry path uses the reserved storage namespace",
            ));
        }
        if let Some(previous) = previous_path.as_ref() {
            if previous >= &canonical {
                return Err(invalid_committed_revision(
                    "directory entry paths are not strictly ordered and unique",
                ));
            }
            if canonical.starts_with(previous) {
                return Err(invalid_committed_revision(
                    "directory entry path conflicts with an ancestor file",
                ));
            }
        }
        previous_path = Some(canonical);

        let start = usize::try_from(entry.chunk_range.0)
            .map_err(|_| invalid_committed_revision("directory range start overflows usize"))?;
        let end = usize::try_from(entry.chunk_range.1)
            .map_err(|_| invalid_committed_revision("directory range end overflows usize"))?;
        if start != next_chunk || end <= start || end > item.chunks.len() {
            return Err(invalid_committed_revision(
                "directory chunk ranges are not contiguous and in bounds",
            ));
        }
        let size = item.chunks[start..end]
            .iter()
            .try_fold(0u64, |sum, chunk| {
                sum.checked_add(chunk.size)
                    .ok_or_else(|| invalid_committed_revision("directory entry size overflows u64"))
            })?;
        if size != entry.size {
            return Err(invalid_committed_revision(
                "directory entry size differs from its chunks",
            ));
        }
        entry.kind.validate().map_err(invalid_committed_revision)?;
        if entry.kind.symlink_target().is_some() && (entry.size != 0 || end - start != 1) {
            return Err(invalid_committed_revision(
                "symlink entry must cover exactly one empty chunk",
            ));
        }
        next_chunk = end;
    }
    if !item.manifest.entries.is_empty() && next_chunk != item.chunks.len() {
        return Err(invalid_committed_revision(
            "directory entries do not cover every chunk",
        ));
    }
    Ok(root)
}

fn validate_revision_authority(artifact_id: &str, path: &PathBuf) -> Result<PathBuf, Error> {
    if artifact_id.is_empty() || artifact_id.len() > 256 {
        return Err(Error::Other(
            "artifact ID must contain between 1 and 256 UTF-8 bytes".to_string(),
        ));
    }
    let path = crate::utils::path::validate_relative(path)?;
    if path.to_str().is_none() {
        return Err(Error::Other(
            "revision logical paths must be valid UTF-8".to_string(),
        ));
    }
    if path.components().next().is_some_and(|component| {
        component.as_os_str() == ".distd-revisions-v1"
            || component.as_os_str() == ".distd-activation-v1"
    }) {
        return Err(Error::Other(
            "revision logical path uses the reserved storage namespace".to_string(),
        ));
    }
    Ok(path)
}

#[derive(Clone, Debug)]
struct ReceivedBacking {
    id: u64,
    allocation_bytes: usize,
}

/// An immutable chunk received from a sync transport.
///
/// Slices share both the underlying bytes and allocation metadata, allowing a
/// cache to charge one bulk allocation exactly once across all retained views.
#[derive(Clone, Debug)]
pub struct ReceivedChunk {
    bytes: Bytes,
    backing: Arc<ReceivedBacking>,
}

impl ReceivedChunk {
    #[must_use]
    pub fn from_vec(data: Vec<u8>) -> Self {
        let allocation_bytes = data.capacity();
        Self {
            bytes: Bytes::from(data),
            backing: Arc::new(ReceivedBacking {
                id: next_received_backing_id(),
                allocation_bytes,
            }),
        }
    }

    #[must_use]
    pub fn from_bytes(data: Bytes) -> Self {
        let allocation_bytes = data.len();
        Self {
            bytes: data,
            backing: Arc::new(ReceivedBacking {
                id: next_received_backing_id(),
                allocation_bytes,
            }),
        }
    }

    #[must_use]
    pub fn copy_from_slice(data: &[u8]) -> Self {
        Self::from_vec(data.to_vec())
    }

    #[must_use]
    pub fn from_static(data: &'static [u8]) -> Self {
        Self {
            bytes: Bytes::from_static(data),
            backing: Arc::new(ReceivedBacking {
                id: next_received_backing_id(),
                allocation_bytes: 0,
            }),
        }
    }

    #[must_use]
    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            bytes: self.bytes.slice(range),
            backing: self.backing.clone(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[doc(hidden)]
    #[must_use]
    pub fn backing_id(&self) -> u64 {
        self.backing.id
    }

    #[doc(hidden)]
    #[must_use]
    pub fn backing_allocation_bytes(&self) -> usize {
        self.backing.allocation_bytes
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes.as_ref().to_vec()
    }
}

impl From<Vec<u8>> for ReceivedChunk {
    fn from(data: Vec<u8>) -> Self {
        Self::from_vec(data)
    }
}

impl From<Bytes> for ReceivedChunk {
    fn from(data: Bytes) -> Self {
        Self::from_bytes(data)
    }
}

impl AsRef<[u8]> for ReceivedChunk {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl Deref for ReceivedChunk {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl PartialEq for ReceivedChunk {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for ReceivedChunk {}

pub mod cas;
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

    #[error("IO error in chunk store")]
    Io(#[from] std::io::Error),

    #[error("Cannot create link")]
    LinkCreation,

    #[error("Cannot reconstruct tree from storage")]
    TreeReconstruct,

    #[error("immutable content-addressed storage key conflicts with supplied content")]
    ImmutableConflict,
}

/// Defines a backend used to store hashes and chunks ad key-value pairs
pub trait ChunkStorage: HashTreeCapable<Arc<Node>, Error> {
    fn get(&self, hash: &Hash) -> Option<Arc<Node>>;
    fn store_chunk(&self, hash: Hash, chunk: &[u8]) -> Result<Arc<Node>, StorageError>;
    fn store_link(
        &self,
        hash: Hash,
        left: Arc<Node>,
        right: Arc<Node>,
    ) -> Result<Arc<Node>, StorageError>;

    fn chunks(&self) -> Vec<Hash>;

    /// Allocated size for all chunks, in bytes
    /// This only counts actual chunks size, excluding any auxiliary structure used by storage backend/adapter
    fn size(&self) -> u64;

    /// Return the ordered list of leaf chunk hashes for a given root.
    /// Default implementation walks the tree.
    fn chunk_list(&self, root: &Hash) -> Vec<Hash> {
        self.get(root)
            .and_then(|node| node.flatten().ok())
            .unwrap_or_default()
    }

    /// Retrieve a chunk by its positional index within a tree.
    /// Default implementation walks to the Nth leaf.
    fn get_chunk_by_index(&self, root: &Hash, index: u32) -> Option<Vec<u8>> {
        let node = self.get(root)?;
        let leaves = node.flatten_iter().ok()?;
        leaves.get(index as usize).map(|arc| (**arc).clone())
    }

    /// Append one positional chunk directly into a caller-owned transfer
    /// buffer. Backends should override this to avoid an intermediate owned
    /// chunk allocation on hot serving paths.
    fn append_chunk_by_index(
        &self,
        root: &Hash,
        index: u32,
        destination: &mut Vec<u8>,
    ) -> Option<usize> {
        let chunk = self.get_chunk_by_index(root, index)?;
        let length = chunk.len();
        destination.extend_from_slice(&chunk);
        Some(length)
    }

    /// Verify that an already-committed immutable revision is complete and
    /// exactly matches its authoritative metadata.
    fn verify_committed_revision(&self, item: &Item) -> Result<(), Error> {
        let root = validate_revision_metadata(item)?;
        for (index, expected) in item.chunks.iter().enumerate() {
            let index = u32::try_from(index)
                .map_err(|_| invalid_committed_revision("chunk index overflows u32"))?;
            let bytes = self
                .get_chunk_by_index(&root.hash, index)
                .ok_or_else(|| invalid_committed_revision(format!("chunk {index} is missing")))?;
            if bytes.len() as u64 != expected.size || hash(&bytes) != expected.hash {
                return Err(invalid_committed_revision(format!(
                    "chunk {index} bytes differ from its immutable identity"
                )));
            }
        }
        Ok(())
    }

    /// Forget an exact revision whose storage commit completed but whose
    /// publication metadata did not. Content-only backends have no Item index
    /// to clean up and therefore need no work.
    fn discard_unpublished_revision(&self, _item: &Item) -> Result<(), Error> {
        Ok(())
    }

    /// Reconcile backend Item indexes with the authoritative published set at
    /// startup. Immutable content may remain for future deduplication.
    fn reconcile_published_revisions(&self, _published: &[Item]) -> Result<(), Error> {
        Ok(())
    }

    //fn drop(hash: Hash); // TODO

    fn insert_chunk(&self, chunk: &[u8]) -> Result<Arc<Node>, StorageError> {
        let hash = hash(chunk);
        tracing::trace!("Insert chunk {hash}, {} bytes", chunk.len());

        self.store_chunk(hash, chunk)
            .inspect(|x| assert!(x.hash() == &hash))
    }

    fn link(&self, left: Arc<Node>, right: Arc<Node>) -> Result<Arc<Node>, StorageError> {
        let hash = merge_hashes(left.hash(), right.hash());
        tracing::trace!("Link {} {} → {}", left.hash(), right.hash(), hash);
        self.store_link(hash, left, right)
            .inspect(|x| assert!(x.hash() == &hash))
    }

    /// Insert bytes into the storage returning the associated hash tree
    fn insert(&self, data: Bytes) -> Result<Arc<Node>, Error>
    where
        Self: Sized,
    {
        self.compute_tree(data.as_ref())
    }

    fn chunk_tree(
        &self,
        data: &[u8],
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<(Arc<Node>, Vec<ChunkInfo>), Error>
    where
        Self: Sized,
    {
        let boundaries = chunk_algorithm.chunk_boundaries(data);
        let mut partials = Vec::with_capacity(boundaries.len().max(1));
        let mut chunks = Vec::with_capacity(boundaries.len().max(1));

        for (offset, length) in boundaries {
            let chunk = &data[offset..offset + length];
            let node = self.insert_chunk(chunk)?;
            let chunk_info = ChunkInfo {
                size: length as u64,
                hash: *node.hash(),
            };
            partials.push(Arc::new(Node::Skipped {
                hash: *node.hash(),
                size: length as u64,
            }));
            chunks.push(chunk_info);
        }

        while partials.len() > 1 {
            let n = partials.len();
            for (to, index) in (0..n - 1).step_by(2).enumerate() {
                partials[to] = self.link(partials[index].clone(), partials[index + 1].clone())?;
            }
            let half = n / 2;
            if n % 2 != 0 {
                partials.swap(half, n - 1);
                partials.truncate(half + 1);
            } else {
                partials.truncate(half);
            }
        }

        Ok((partials.swap_remove(0), chunks))
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
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        let (hash_tree, chunks) = self.chunk_tree(file.as_ref(), chunk_algorithm)?;
        Ok(Item::make(
            name,
            path,
            revision,
            description,
            hash_tree.chunk_info(),
            chunks,
            chunk_algorithm,
        )?)
    }

    fn create_item_from_files(
        &self,
        _name: ItemName,
        _path: PathBuf,
        _revision: u32,
        _description: Option<String>,
        _files: Vec<(PathBuf, Bytes)>,
        _chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        Err(Error::Other(
            "multi-file artifacts are unsupported by this storage".to_string(),
        ))
    }

    /// Build the exact revision metadata without mutating storage.
    fn prepare_revision(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        file: Bytes,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        let path = validate_revision_authority(&artifact_id, &path)?;
        let chunks = prepared_chunks(file.as_ref(), chunk_algorithm);
        let root = prepared_tree(&chunks)?;
        Ok(Item::make(
            name,
            path,
            revision,
            description,
            root,
            chunks,
            chunk_algorithm,
        )?
        .with_artifact_id(artifact_id))
    }

    /// Build exact multi-file revision metadata without mutating storage.
    fn prepare_revision_from_files(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        files: Vec<(PathBuf, Bytes)>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        self.prepare_revision_from_entries(
            artifact_id,
            name,
            path,
            revision,
            description,
            files,
            BTreeMap::new(),
            chunk_algorithm,
        )
    }

    /// Build exact multi-file revision metadata with explicit entry kinds.
    ///
    /// Paths absent from `kinds` are regular files. A symbolic link is supplied as an empty
    /// file whose kind names its target; every key of `kinds` must name a supplied file.
    fn prepare_revision_from_entries(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        files: Vec<(PathBuf, Bytes)>,
        kinds: BTreeMap<PathBuf, EntryKind>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        let path = validate_revision_authority(&artifact_id, &path)?;
        if files.is_empty() {
            return Err(Error::MissingData);
        }
        let mut files = files
            .into_iter()
            .map(|(relative_path, data)| {
                let relative_path = crate::utils::path::validate_relative(&relative_path)?;
                if relative_path.to_str().is_none() {
                    return Err(Error::Other(
                        "directory revision paths must be valid UTF-8".to_string(),
                    ));
                }
                if relative_path.components().next().is_some_and(|component| {
                    component.as_os_str() == std::ffi::OsStr::new(".distd-revisions-v1")
                        || component.as_os_str() == std::ffi::OsStr::new(".distd-activation-v1")
                }) {
                    return Err(Error::Other(
                        "directory revision path uses the reserved storage namespace".to_string(),
                    ));
                }
                Ok((relative_path, data))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        files.sort_by(|left, right| left.0.cmp(&right.0));
        if files.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(Error::Other(
                "directory revision contains duplicate paths".to_string(),
            ));
        }
        let kinds = kinds
            .into_iter()
            .map(|(relative_path, kind)| {
                Ok((crate::utils::path::validate_relative(&relative_path)?, kind))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;
        self.prepare_revision_from_sorted_files(
            artifact_id,
            name,
            path,
            revision,
            description,
            &files,
            &kinds,
            chunk_algorithm,
        )
    }

    /// Build exact multi-file revision metadata from caller-owned files that
    /// are already in canonical path order, without cloning their path table.
    fn prepare_revision_from_sorted_files(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        files: &[(PathBuf, Bytes)],
        kinds: &BTreeMap<PathBuf, EntryKind>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        let path = validate_revision_authority(&artifact_id, &path)?;
        if files.is_empty() {
            return Err(Error::MissingData);
        }
        let mut matched_kinds = 0usize;
        let mut chunks = Vec::new();
        let mut entries = Vec::with_capacity(files.len());
        let mut previous: Option<&PathBuf> = None;
        for (relative_path, data) in files {
            let canonical = crate::utils::path::validate_relative(relative_path)?;
            if &canonical != relative_path || relative_path.to_str().is_none() {
                return Err(Error::Other(
                    "directory revision paths must be canonical UTF-8".to_string(),
                ));
            }
            if relative_path.components().next().is_some_and(|component| {
                component.as_os_str() == std::ffi::OsStr::new(".distd-revisions-v1")
                    || component.as_os_str() == std::ffi::OsStr::new(".distd-activation-v1")
            }) {
                return Err(Error::Other(
                    "directory revision path uses the reserved storage namespace".to_string(),
                ));
            }
            if previous.is_some_and(|previous| previous >= relative_path) {
                return Err(Error::Other(
                    "directory revision paths must be strictly ordered and unique".to_string(),
                ));
            }
            previous = Some(relative_path);
            let kind = match kinds.get(relative_path) {
                Some(kind) => {
                    matched_kinds += 1;
                    kind.clone()
                }
                None => EntryKind::Regular,
            };
            kind.validate()
                .map_err(|reason| Error::Other(reason.to_string()))?;
            if kind.symlink_target().is_some() && !data.is_empty() {
                return Err(Error::Other(
                    "symlink entry must not carry file data".to_string(),
                ));
            }
            let relative_path = relative_path
                .to_str()
                .expect("validated directory revision path is UTF-8");
            let start = u32::try_from(chunks.len())
                .map_err(|_| Error::Other("revision has too many chunks".to_string()))?;
            chunks.extend(prepared_chunks(data.as_ref(), chunk_algorithm));
            let end = u32::try_from(chunks.len())
                .map_err(|_| Error::Other("revision has too many chunks".to_string()))?;
            entries.push(FileEntry {
                relative_path: relative_path.to_string(),
                size: data.len() as u64,
                chunk_range: (start, end),
                kind,
            });
        }
        if matched_kinds != kinds.len() {
            return Err(Error::Other(
                "entry kind names a path that is not in the revision".to_string(),
            ));
        }
        let root = prepared_tree(&chunks)?;
        Ok(Item::make_with_entries(
            name,
            path,
            revision,
            description,
            root,
            chunks,
            entries,
            chunk_algorithm,
        )?
        .with_artifact_id(artifact_id))
    }

    /// Durably store a previously prepared single-file revision.
    fn commit_revision(&self, item: Item, file: Bytes) -> Result<Item, Error>
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
        if !same_revision(&item, &expected) {
            return Err(Error::Other(
                "prepared revision does not match supplied bytes".to_string(),
            ));
        }
        let (root, chunks) = self.chunk_tree(file.as_ref(), item.manifest.chunk_algorithm)?;
        if root.chunk_info() != item.metadata.root || chunks != item.chunks {
            return Err(Error::Other(
                "stored revision does not match prepared metadata".to_string(),
            ));
        }
        Ok(item)
    }

    /// Durably store a previously prepared multi-file revision.
    fn commit_revision_from_files(
        &self,
        item: Item,
        mut files: Vec<(PathBuf, Bytes)>,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        let expected = self.prepare_revision_from_entries(
            item.metadata.artifact_id.clone(),
            item.metadata.name.clone(),
            item.metadata.path.clone(),
            item.metadata.revision,
            item.metadata.description.clone(),
            files.clone(),
            entry_kinds(&item),
            item.manifest.chunk_algorithm,
        )?;
        if !same_revision(&item, &expected) {
            return Err(Error::Other(
                "prepared directory revision does not match supplied files".to_string(),
            ));
        }
        files.sort_by(|left, right| left.0.cmp(&right.0));
        let mut partials = Vec::new();
        for (_, data) in files {
            if data.is_empty() {
                partials.push(self.insert_chunk(&[])?);
                continue;
            }
            for (offset, length) in item
                .manifest
                .chunk_algorithm
                .chunk_boundaries(data.as_ref())
            {
                partials.push(self.insert_chunk(&data[offset..offset + length])?);
            }
        }
        while partials.len() > 1 {
            let n = partials.len();
            for (to, index) in (0..n - 1).step_by(2).enumerate() {
                partials[to] = self.link(partials[index].clone(), partials[index + 1].clone())?;
            }
            let half = n / 2;
            if n % 2 != 0 {
                partials.swap(half, n - 1);
                partials.truncate(half + 1);
            } else {
                partials.truncate(half);
            }
        }
        let root = partials.pop().ok_or(Error::MissingData)?;
        if root.chunk_info() != item.metadata.root {
            return Err(Error::Other(
                "stored directory revision does not match prepared metadata".to_string(),
            ));
        }
        Ok(item)
    }

    /// Build a new Item from its metadata and root node
    fn build_item(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        root: Arc<Node>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        Ok(Item::new(
            name,
            path,
            revision,
            description,
            &root,
            chunk_algorithm,
        ))
    }

    /// Build a new Item from its metadata and a streaming of nodes
    fn receive_item<T>(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        mut stream: T,
        chunk_algorithm: ChunkAlgorithm,
    ) -> impl std::future::Future<Output = Result<Item, crate::error::Error>> + Send
    where
        Self: Sized + Send + Sync,
        T: Stream<Item = Node> + std::marker::Unpin + Send,
    {
        async move {
            let mut n = None; // final node
            let mut i = 0; // node counter
            while let Some(node) = stream.next().await {
                n = Some(self.try_fill_in(&node)?);
                i += 1;
            }

            let n = n.ok_or(StorageError::TreeReconstruct)?;
            tracing::trace!("Reconstructed {i} nodes with {} bytes total", n.size());

            Ok(Item::new(
                name,
                path,
                revision,
                description,
                &n,
                chunk_algorithm,
            ))
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
    fn try_fill_in(&self, tree: &Node) -> Result<Arc<Node>, StorageError> {
        tracing::trace!("Filling {}", tree.hash());
        Ok(match tree {
            Node::Stored { hash, data } => self.store_chunk(*hash, data)?,
            Node::Parent { left, right, .. } => {
                let l = self.try_fill_in(left)?;
                let r = self.try_fill_in(right)?;
                self.link(l, r)?
            }
            Node::Skipped { hash, .. } => self.get(hash).ok_or(StorageError::TreeReconstruct)?,
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
    use std::sync::atomic::AtomicU64;

    use crate::{chunk_storage::hashmap_storage::HashMapStorage, chunks::CHUNK_SIZE, hash::hash};

    fn prepare_kinded(
        files: Vec<(&str, &'static [u8])>,
        kinds: Vec<(&str, EntryKind)>,
    ) -> Result<Item, Error> {
        HashMapStorage::default().prepare_revision_from_entries(
            "kinded".to_string(),
            "kinded".to_string(),
            PathBuf::from("kinded"),
            0,
            None,
            files
                .into_iter()
                .map(|(path, data)| (PathBuf::from(path), Bytes::from_static(data)))
                .collect(),
            kinds
                .into_iter()
                .map(|(path, kind)| (PathBuf::from(path), kind))
                .collect(),
            ChunkAlgorithm::default(),
        )
    }

    #[test]
    fn entry_kinds_are_prepared_as_empty_chunk_links_and_validated() {
        let link = |target: &str| EntryKind::Symlink {
            target: target.to_string(),
        };
        let item = prepare_kinded(
            vec![
                ("bin/tool", b"#!/bin/sh\n"),
                ("bin/tool.sh", b""),
                ("doc", b"x"),
            ],
            vec![
                ("bin/tool", EntryKind::Executable),
                ("bin/tool.sh", link("tool")),
            ],
        )
        .unwrap();
        validate_revision_metadata(&item).unwrap();
        let kinds: Vec<_> = item
            .manifest
            .entries
            .iter()
            .map(|entry| (entry.relative_path.as_str(), entry.kind.clone()))
            .collect();
        assert_eq!(
            kinds,
            [
                ("bin/tool", EntryKind::Executable),
                ("bin/tool.sh", link("tool")),
                ("doc", EntryKind::Regular),
            ]
        );
        let link_entry = &item.manifest.entries[1];
        assert_eq!(link_entry.size, 0);
        assert_eq!(
            item.chunks[link_entry.chunk_range.0 as usize..link_entry.chunk_range.1 as usize],
            [ChunkInfo {
                hash: hash(&[]),
                size: 0
            }]
        );
        assert_eq!(entry_kinds(&item).len(), 2);

        assert!(prepare_kinded(vec![("link", b"data")], vec![("link", link("x"))]).is_err());
        assert!(prepare_kinded(vec![("file", b"data")], vec![("missing", link("x"))]).is_err());
        for target in ["", "a\0b"] {
            assert!(prepare_kinded(vec![("link", b"")], vec![("link", link(target))]).is_err());
        }
        assert!(prepare_kinded(
            vec![("link", b"")],
            vec![(
                "link",
                link(&"t".repeat(crate::item::MAX_SYMLINK_TARGET_BYTES + 1))
            )]
        )
        .is_err());

        let mut sized_link = item.clone();
        sized_link.manifest.entries[2].kind = link("tool");
        assert!(validate_revision_metadata(&sized_link).is_err());
    }

    #[test]
    fn received_backing_identity_exhaustion_is_fail_stop() {
        let counter = AtomicU64::new(u64::MAX - 1);
        assert_eq!(next_received_backing_id_from(&counter), u64::MAX - 1);
        assert!(std::panic::catch_unwind(|| next_received_backing_id_from(&counter)).is_err());
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
    }

    struct VerificationStorage {
        chunks: Vec<Option<Vec<u8>>>,
    }

    impl ChunkStorage for VerificationStorage {
        fn get(&self, _hash: &Hash) -> Option<Arc<Node>> {
            None
        }

        fn store_chunk(&self, _hash: Hash, _chunk: &[u8]) -> Result<Arc<Node>, StorageError> {
            Err(StorageError::ChunkInsertError)
        }

        fn store_link(
            &self,
            _hash: Hash,
            _left: Arc<Node>,
            _right: Arc<Node>,
        ) -> Result<Arc<Node>, StorageError> {
            Err(StorageError::LinkCreation)
        }

        fn chunks(&self) -> Vec<Hash> {
            Vec::new()
        }

        fn size(&self) -> u64 {
            0
        }

        fn get_chunk_by_index(&self, _root: &Hash, index: u32) -> Option<Vec<u8>> {
            self.chunks.get(index as usize).cloned().flatten()
        }
    }

    impl HashTreeCapable<Arc<Node>, Error> for VerificationStorage {
        fn func(&self, _data: &[u8]) -> Result<Arc<Node>, Error> {
            Err(Error::Storage(StorageError::ChunkInsertError))
        }

        fn merge(&self, _left: &Arc<Node>, _right: &Arc<Node>) -> Result<Arc<Node>, Error> {
            Err(Error::Storage(StorageError::LinkCreation))
        }
    }

    #[test]
    fn hashmap_verifies_exact_committed_revision_and_rejects_missing() {
        let storage = HashMapStorage::default();
        let data = Bytes::from_static(b"abcdefghijkl");
        let algorithm = ChunkAlgorithm::fixed_aligned(4, 1);
        let item = storage
            .prepare_revision(
                "artifact".to_string(),
                "artifact".to_string(),
                PathBuf::from("artifact.bin"),
                1,
                None,
                data.clone(),
                algorithm,
            )
            .unwrap();
        assert!(storage.verify_committed_revision(&item).is_err());

        let item = storage.commit_revision(item, data).unwrap();
        storage.verify_committed_revision(&item).unwrap();
    }

    #[test]
    fn committed_revision_verification_rejects_wrong_bytes_order_and_root() {
        let preparation = HashMapStorage::default();
        let data = Bytes::from_static(b"abcdefghijkl");
        let item = preparation
            .prepare_revision(
                "artifact".to_string(),
                "artifact".to_string(),
                PathBuf::from("artifact.bin"),
                1,
                None,
                data,
                ChunkAlgorithm::fixed_aligned(4, 1),
            )
            .unwrap();
        let exact = vec![
            Some(b"abcd".to_vec()),
            Some(b"efgh".to_vec()),
            Some(b"ijkl".to_vec()),
        ];
        VerificationStorage {
            chunks: exact.clone(),
        }
        .verify_committed_revision(&item)
        .unwrap();

        let mut missing = exact.clone();
        missing[1] = None;
        assert!(VerificationStorage { chunks: missing }
            .verify_committed_revision(&item)
            .is_err());

        let mut wrong = exact.clone();
        wrong[1] = Some(b"xxxx".to_vec());
        assert!(VerificationStorage { chunks: wrong }
            .verify_committed_revision(&item)
            .is_err());

        let mut reordered = exact;
        reordered.swap(0, 1);
        assert!(VerificationStorage { chunks: reordered }
            .verify_committed_revision(&item)
            .is_err());

        let mut forged_root = item;
        forged_root.manifest.root_hash = hash(b"forged root");
        assert!(VerificationStorage {
            chunks: vec![
                Some(b"abcd".to_vec()),
                Some(b"efgh".to_vec()),
                Some(b"ijkl".to_vec()),
            ],
        }
        .verify_committed_revision(&forged_root)
        .is_err());
    }

    #[test]
    fn revision_metadata_rejects_file_ancestor_path_conflicts() {
        let storage = HashMapStorage::default();
        let mut item = storage
            .prepare_revision(
                "artifact".to_string(),
                "artifact".to_string(),
                PathBuf::from("artifact"),
                1,
                None,
                Bytes::from_static(b"abcdefgh"),
                ChunkAlgorithm::fixed_aligned(4, 1),
            )
            .unwrap();
        item.manifest.entries = vec![
            FileEntry {
                relative_path: "a".to_string(),
                size: 4,
                chunk_range: (0, 1),
                kind: EntryKind::Regular,
            },
            FileEntry {
                relative_path: "a/b".to_string(),
                size: 4,
                chunk_range: (1, 2),
                kind: EntryKind::Regular,
            },
        ];

        assert!(validate_revision_metadata(&item).is_err());
    }

    pub fn single_chunk_insertion<S>(s: &S)
    where
        S: ChunkStorage,
    {
        let data = Bytes::from_static(b"very few bytes");
        let len = data.len() as u64;
        s.insert(data).unwrap();
        assert_eq!(len, s.size());
    }

    /// Multiple chunks, not aligned with `CHUNK_SIZE`
    pub fn multiple_chunks_insertion<S>(s: &S)
    where
        S: ChunkStorage,
    {
        let data = Bytes::from_static(include_bytes!("../../Cargo.lock"));
        let len = data.len() as u64;
        println!("\nOriginal lenght: {}, stored length: {}", len, s.size());
        println!();
        assert!(len >= s.size());
    }

    pub fn chunks_deduplication<S>(s: &S)
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

        let hash_vec = root.flatten().unwrap();
        assert_eq!(hash_vec.len(), 3);
        assert_eq!(hash_vec[0], zeros_chunk_hash);
        assert_eq!(hash_vec[1], zeros_chunk_hash);
        assert_eq!(hash_vec[2], zeros_chunk_hash);

        let hash_set = root.hashes();
        assert_eq!(hash_set.len(), 1);
        for i in hash_set {
            assert_eq!(i, zeros_chunk_hash);
        }

        let cloned = root.clone_data().unwrap();
        assert_eq!(cloned.len(), SIZE);
        for b in cloned {
            assert_eq!(b, 0u8);
        }
    }

    pub fn storage_2mb<S>(s: &S)
    where
        S: ChunkStorage,
    {
        let mut data = BytesMut::with_capacity(2_000_000);
        rand::rngs::OsRng.fill_bytes(&mut data);

        let len = data.len() as u64;
        let root = s.insert(data.clone().into()).unwrap();
        //print_tree(&*root.to_owned()).unwrap();
        assert!(len >= s.size());

        let cloned = root.clone_data().unwrap();
        for (i, b) in cloned.iter().enumerate() {
            //println!("{} {} {}", i, data[i], *b);
            assert_eq!(data[i], *b);
        }
    }

    /// 3 chunks (odd) with distinct random data — regression test for compute_tree
    pub fn storage_3_chunks<S>(s: &S)
    where
        S: ChunkStorage,
    {
        let size = CHUNK_SIZE * 2 + CHUNK_SIZE / 2; // 2.5 chunks
        let mut data = BytesMut::zeroed(size);
        rand::rngs::OsRng.fill_bytes(&mut data);

        let root = s.insert(data.clone().into()).unwrap();
        assert_eq!(root.size(), size as u64);
        let cloned = root.clone_data().unwrap();
        assert_eq!(cloned.len(), size);
        assert_eq!(&cloned[..], &data[..]);
    }

    /// 5 chunks (odd) with distinct random data — regression test for compute_tree
    pub fn storage_5_chunks<S>(s: &S)
    where
        S: ChunkStorage,
    {
        let size = CHUNK_SIZE * 4 + CHUNK_SIZE / 3; // 4.33 chunks → 5
        let mut data = BytesMut::zeroed(size);
        rand::rngs::OsRng.fill_bytes(&mut data);

        let root = s.insert(data.clone().into()).unwrap();
        assert_eq!(root.size(), size as u64);
        let cloned = root.clone_data().unwrap();
        assert_eq!(cloned.len(), size);
        assert_eq!(&cloned[..], &data[..]);
    }

    /// 7 chunks (odd) with distinct random data — regression test for compute_tree
    pub fn storage_7_chunks<S>(s: &S)
    where
        S: ChunkStorage,
    {
        let size = CHUNK_SIZE * 6 + CHUNK_SIZE / 2; // 6.5 chunks → 7
        let mut data = BytesMut::zeroed(size);
        rand::rngs::OsRng.fill_bytes(&mut data);

        let root = s.insert(data.clone().into()).unwrap();
        assert_eq!(root.size(), size as u64);
        let cloned = root.clone_data().unwrap();
        assert_eq!(cloned.len(), size);
        assert_eq!(&cloned[..], &data[..]);
    }

    macro_rules! chunk_storage_tests {
        ($t:ty, $builder:ident) => {
            crate::chunk_storage::tests::chunk_storage_tests!($t, single_chunk_insertion, $builder);
            crate::chunk_storage::tests::chunk_storage_tests!(
                $t,
                multiple_chunks_insertion,
                $builder
            );
            crate::chunk_storage::tests::chunk_storage_tests!($t, chunks_deduplication, $builder);
            crate::chunk_storage::tests::chunk_storage_tests!($t, storage_2mb, $builder);
            crate::chunk_storage::tests::chunk_storage_tests!($t, storage_3_chunks, $builder);
            crate::chunk_storage::tests::chunk_storage_tests!($t, storage_5_chunks, $builder);
            crate::chunk_storage::tests::chunk_storage_tests!($t, storage_7_chunks, $builder);
            // ... any more tests go here ...
        };
        ($t:ty, $name:ident, $builder:ident) => {
            #[test]
            fn $name() {
                $crate::chunk_storage::tests::$name::<$t>(&$builder());
            }
        };
    }

    pub(crate) use chunk_storage_tests;
}
