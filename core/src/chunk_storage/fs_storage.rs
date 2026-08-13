use std::{
    cmp::Ordering,
    collections::VecDeque,
    fs::{self, create_dir_all, remove_file, File, OpenOptions},
    io::{BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
};

use serde::{Deserialize, Serialize};
use tokio_stream::{Stream, StreamExt};

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::{
    chunk_storage::StorageError,
    chunks::{ChunkAlgorithm, ChunkInfo, CHUNK_SIZE},
    durable_state::{
        read_bounded_file, read_enveloped_state, replace_enveloped_state, DurableStateKind,
        MAX_DURABLE_STATE_PAYLOAD,
    },
    error::{Error, InvalidParameter},
    hash::{hash as do_hash, merge_hashes, Hash, HashTreeCapable},
    item::{FileEntry, Item, Manifest, Name as ItemName},
    utils::settings::cache_dir,
};

use super::{ChunkStorage, Node, ReceivedChunk};

const DEFAULT_CHUNK_CACHE_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_TREE_CACHE_ENTRIES: usize = 65_536;
const FS_STORAGE_PAYLOAD_SCHEMA: u16 = 1;
const FS_STORAGE_ROOT_DOMAIN: &[u8] = b"distd fs storage root identity v1\0";
const FS_STORAGE_REVISION_NAMESPACE: &str = ".distd-revisions-v1";
const FS_STORAGE_MAX_LINK_RECORDS: usize = 65_536;
const FS_STORAGE_MAX_LINK_DEPTH: usize = 256;

#[derive(Clone, Debug)]
enum ChunkData {
    Owned(Arc<Vec<u8>>),
    Shared(ReceivedChunk),
}

impl ChunkData {
    fn into_owned(self) -> Arc<Vec<u8>> {
        match self {
            Self::Owned(data) => data,
            Self::Shared(data) => Arc::new(data.into_vec()),
        }
    }
}

impl AsRef<[u8]> for ChunkData {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Owned(data) => data.as_ref(),
            Self::Shared(data) => data.as_ref(),
        }
    }
}

#[derive(Debug)]
struct SharedBackingUsage {
    allocation_bytes: usize,
    cached_views: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct FsStorageCacheConfig {
    pub max_chunk_bytes: usize,
    pub max_tree_entries: usize,
}

impl Default for FsStorageCacheConfig {
    fn default() -> Self {
        Self {
            max_chunk_bytes: DEFAULT_CHUNK_CACHE_BYTES,
            max_tree_entries: DEFAULT_TREE_CACHE_ENTRIES,
        }
    }
}

#[derive(Debug, Default)]
struct HotChunkCache {
    max_bytes: usize,
    total_bytes: usize,
    order: VecDeque<Hash>,
    entries: HashMap<Hash, ChunkData>,
    shared_backings: HashMap<u64, SharedBackingUsage>,
}

#[derive(Debug, Default)]
struct HotNodeCache {
    max_entries: usize,
    order: VecDeque<Hash>,
    entries: HashMap<Hash, Arc<Node>>,
}

impl HotNodeCache {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            order: VecDeque::new(),
            entries: HashMap::default(),
        }
    }

    fn get(&mut self, hash: &Hash) -> Option<Arc<Node>> {
        let node = self.entries.get(hash)?.clone();
        self.order.push_back(*hash);
        Some(node)
    }

    fn insert(&mut self, hash: Hash, node: Arc<Node>) {
        if self.max_entries == 0 {
            return;
        }

        self.entries.insert(hash, node);
        self.order.push_back(hash);

        while self.entries.len() > self.max_entries {
            let Some(oldest_hash) = self.order.pop_front() else {
                break;
            };
            let remove = self
                .entries
                .get(&oldest_hash)
                .map(|current| Arc::strong_count(current) == 1)
                .unwrap_or(false);
            if remove {
                self.entries.remove(&oldest_hash);
            }
        }
    }
}

impl HotChunkCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            total_bytes: 0,
            order: VecDeque::new(),
            entries: HashMap::default(),
            shared_backings: HashMap::default(),
        }
    }

    fn get(&mut self, hash: &Hash) -> Option<ChunkData> {
        let data = self.entries.get(hash)?.clone();
        self.order.push_back(*hash);
        Some(data)
    }

    fn insert_owned(&mut self, hash: Hash, data: Arc<Vec<u8>>) {
        self.insert(hash, ChunkData::Owned(data));
    }

    fn insert_shared(&mut self, hash: Hash, data: ReceivedChunk) {
        self.insert(hash, ChunkData::Shared(data));
    }

    fn insert(&mut self, hash: Hash, data: ChunkData) {
        if self.max_bytes == 0 {
            return;
        }

        let allocation_bytes = match &data {
            ChunkData::Owned(data) => data.len(),
            ChunkData::Shared(data) => data.backing_allocation_bytes(),
        };
        if allocation_bytes > self.max_bytes {
            return;
        }

        if let Some(old) = self.entries.remove(&hash) {
            self.uncharge(&old);
            self.order.retain(|queued| queued != &hash);
        }

        self.charge(&data);
        self.entries.insert(hash, data);
        self.order.push_back(hash);

        while self.total_bytes > self.max_bytes {
            let Some(oldest_hash) = self.order.pop_front() else {
                break;
            };
            let remove = match self.entries.get(&oldest_hash) {
                Some(ChunkData::Owned(current)) => Arc::strong_count(current) == 1,
                Some(ChunkData::Shared(_)) => true,
                None => false,
            };
            if remove {
                if let Some(old) = self.entries.remove(&oldest_hash) {
                    self.uncharge(&old);
                }
            }
        }
    }

    fn charge(&mut self, data: &ChunkData) {
        match data {
            ChunkData::Owned(data) => {
                self.total_bytes = self.total_bytes.saturating_add(data.len());
            }
            ChunkData::Shared(data) => {
                let id = data.backing_id();
                if let Some(usage) = self.shared_backings.get_mut(&id) {
                    usage.cached_views += 1;
                } else {
                    let allocation_bytes = data.backing_allocation_bytes();
                    self.total_bytes = self.total_bytes.saturating_add(allocation_bytes);
                    self.shared_backings.insert(
                        id,
                        SharedBackingUsage {
                            allocation_bytes,
                            cached_views: 1,
                        },
                    );
                }
            }
        }
    }

    fn uncharge(&mut self, data: &ChunkData) {
        match data {
            ChunkData::Owned(data) => {
                self.total_bytes = self.total_bytes.saturating_sub(data.len());
            }
            ChunkData::Shared(data) => {
                let id = data.backing_id();
                let remove = self
                    .shared_backings
                    .get_mut(&id)
                    .map(|usage| {
                        usage.cached_views -= 1;
                        usage.cached_views == 0
                    })
                    .unwrap_or(false);
                if remove {
                    if let Some(usage) = self.shared_backings.remove(&id) {
                        self.total_bytes = self.total_bytes.saturating_sub(usage.allocation_bytes);
                    }
                }
            }
        }
    }
}

pub fn open_file(path: &Path) -> Result<File, Error> {
    File::options()
        .create(true)
        .read(true)
        .write(true)
        .append(false)
        .truncate(false)
        .open(path)
        .inspect_err(|e| tracing::error!("Cannot create file at {:?}: {}", path, e))
        .map_err(Error::IoError)
}

fn open_existing_file(path: &Path) -> Result<File, Error> {
    File::options()
        .read(true)
        .write(true)
        .append(false)
        .truncate(false)
        .open(path)
        .inspect_err(|e| tracing::error!("Cannot open existing file at {:?}: {}", path, e))
        .map_err(Error::IoError)
}

#[derive(Debug)]
struct Handle {
    pub buf_writer: BufWriter<File>,
    pub position: u64,
}

impl Handle {
    pub fn new(path: &Path) -> Result<Self, Error> {
        Ok(Self {
            buf_writer: BufWriter::with_capacity(CHUNK_SIZE * 8, open_file(path)?),
            position: 0,
        })
    }

    fn existing(path: &Path) -> Result<Self, Error> {
        Ok(Self {
            buf_writer: BufWriter::with_capacity(CHUNK_SIZE * 8, open_existing_file(path)?),
            position: 0,
        })
    }

    pub fn write(&mut self, chunk: &[u8], offset: u64) -> Result<(), Error> {
        if self.position != offset {
            self.buf_writer.seek(std::io::SeekFrom::Start(offset))?;
            self.position = offset;
        }
        self.buf_writer.write_all(chunk).map_err(Error::IoError)?;
        self.position += u64::try_from(chunk.len()).map_err(InvalidParameter::from)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        self.buf_writer.flush().map_err(Error::IoError)
    }

    /// Read `buf.len()` bytes from the underlying file at `offset`.
    /// Flushes any pending writes first so they are visible.
    pub fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), Error> {
        self.buf_writer.flush().map_err(Error::IoError)?;
        let file = self.buf_writer.get_mut();
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.read_exact(buf).map_err(Error::IoError)?;
        // Mark position as unknown so next write re-seeks
        self.position = offset + buf.len() as u64;
        Ok(())
    }

    pub fn set_len(&mut self, length: u64) -> Result<(), Error> {
        self.buf_writer.flush().map_err(Error::IoError)?;
        self.buf_writer
            .get_mut()
            .set_len(length)
            .map_err(Error::IoError)
    }
}

/// Chunk stored in multiple files
/// Basically ref-counting on items paths
#[derive(Debug, Clone, Serialize, Deserialize)]
struct InFileChunk {
    pub info: ChunkInfo,
    pub path: PathBuf,
    pub offset: u64,
    pub populated: Arc<AtomicBool>,
}

impl TryFrom<InFileChunk> for Node {
    type Error = Error;

    /// Try to read the chunk from the file at first path
    ///
    /// If the chunk is not populated, it will return an error
    fn try_from(value: InFileChunk) -> Result<Self, Self::Error> {
        if !value.populated.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::MissingData);
        }
        let mut file = File::open(&value.path)?;
        file.seek(std::io::SeekFrom::Start(value.offset))?;

        let mut buf = vec![0u8; usize::try_from(value.info.size).map_err(InvalidParameter::from)?];

        file.read_exact(&mut buf)?;

        Ok(Node::Stored {
            hash: value.info.hash,
            data: Arc::new(buf),
        })
    }
}

impl TryFrom<&Arc<InFileChunk>> for Node {
    type Error = Error;
    fn try_from(value: &Arc<InFileChunk>) -> Result<Self, Self::Error> {
        Self::try_from((**value).clone())
    }
}

impl TryFrom<&InFileChunk> for Node {
    type Error = Error;
    fn try_from(value: &InFileChunk) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

impl TryFrom<&mut InFileChunk> for Node {
    type Error = Error;
    fn try_from(value: &mut InFileChunk) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

impl InFileChunk {
    /// Write a chunk to the file at all the registered paths for that chunk
    pub fn write(&self, hash: &Hash, chunk: &[u8], handle: &mut Handle) -> Result<(), Error> {
        tracing::trace!(
            "Writing {hash}, {} bytes at {}, {} offset",
            chunk.len(),
            self.path.to_string_lossy(),
            self.offset,
        );
        assert_eq!(&self.info.hash, hash);

        if self.populated.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!("Already populated {hash}, skipping");
            return Ok(());
        }

        let mut count = 0;

        // write chunk to all associated files (and offsets)
        handle
            .write(chunk, self.offset)
            .map(|()| {
                self.populated
                    .swap(true, std::sync::atomic::Ordering::Relaxed);
            })
            .inspect(|()| count += chunk.len())
            .inspect(|()| tracing::trace!("{count} bytes written"))
            .inspect_err(|e| tracing::error!("Failed writing {hash} after {count} bytes: {e}"))
    }
}

// ─── Inner serialisable state ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FsStorageChunkV1 {
    info: ChunkInfo,
    relative_path: PathBuf,
    offset: u64,
    populated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FsStorageDataEntryV1 {
    hash: Hash,
    chunks: Vec<FsStorageChunkV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FsStorageLinkEntryV1 {
    hash: Hash,
    nodes: Vec<FsStorageNodeV1>,
}

/// Post-order flat node records. Parent indices must refer only to earlier
/// records, so payload decoding never recurses through attacker-controlled
/// bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum FsStorageNodeV1 {
    Parent {
        hash: Hash,
        size: u64,
        left: u32,
        right: u32,
    },
    Skipped {
        hash: Hash,
        size: u64,
    },
}

/// Canonical schema-1 payload. Constructor-owned paths and runtime-only state
/// deliberately do not appear here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FsStorageStateV1 {
    root_identity: [u8; 32],
    items: Vec<Item>,
    data: Vec<FsStorageDataEntryV1>,
    links: Vec<FsStorageLinkEntryV1>,
}

/// Frozen reader for the only supported pre-envelope layout.
/// Field order and field types match `FsStorageState` at a5d7c997 exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyFsStorageState {
    root: PathBuf,
    items: HashSet<Item>,
    persistance_path: PathBuf,
    data: HashMap<Hash, Vec<Arc<InFileChunk>>>,
    links: HashMap<Hash, Arc<Node>>,
}

fn invalid_state(message: impl Into<String>) -> Error {
    Error::Other(format!(
        "invalid filesystem-storage state: {}",
        message.into()
    ))
}

fn bitcode_identity<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    bitcode::serialize(value).map_err(|error| InvalidParameter::from(error).into())
}

fn sort_by_bitcode_identity<T: Serialize + Clone>(values: &mut Vec<T>) -> Result<(), Error> {
    let mut identified = values
        .drain(..)
        .map(|value| Ok((bitcode_identity(&value)?, value)))
        .collect::<Result<Vec<_>, Error>>()?;
    identified.sort_by(|left, right| left.0.cmp(&right.0));
    if identified.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(invalid_state("duplicate encoded identity"));
    }
    values.extend(identified.into_iter().map(|(_, value)| value));
    Ok(())
}

fn require_canonical_order<T: Serialize>(values: &[T]) -> Result<(), Error> {
    let identities = values
        .iter()
        .map(bitcode_identity)
        .collect::<Result<Vec<_>, _>>()?;
    if identities
        .windows(2)
        .any(|pair| pair[0].cmp(&pair[1]) != Ordering::Less)
    {
        return Err(invalid_state(
            "encoded identities are duplicated or not in canonical order",
        ));
    }
    Ok(())
}

fn canonical_root_bytes(root: &Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        root.as_os_str().as_bytes().to_vec()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        root.as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect()
    }
    #[cfg(not(any(unix, windows)))]
    {
        root.as_os_str().to_string_lossy().as_bytes().to_vec()
    }
}

fn fs_storage_root_identity(root: &Path) -> Result<[u8; 32], Error> {
    let root = fs::canonicalize(root)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(FS_STORAGE_ROOT_DOMAIN);
    hasher.update(&canonical_root_bytes(&root));
    Ok(*hasher.finalize().as_bytes())
}

fn identity_filename(identity: &[u8; 32]) -> String {
    let mut name = String::with_capacity(64 + ".state".len());
    for byte in identity {
        use std::fmt::Write as _;
        write!(&mut name, "{byte:02x}").expect("writing to String cannot fail");
    }
    name.push_str(".state");
    name
}

/// All mutable state for [`FsStorage`], kept behind an `RwLock` for interior mutability.
///
/// This is the type that is persisted to disk; [`FsStorage`] itself is a thin wrapper.
#[derive(Debug, Default, Serialize, Deserialize)]
struct FsStorageState {
    /// Root directory where items are stored
    pub root: PathBuf,
    /// Items, used to track the known items and their chunk layout
    pub items: HashSet<Item>,
    /// Path where to store persistent data
    persistance_path: PathBuf,
    /// Per-chunk location on disk
    data: HashMap<Hash, Vec<Arc<InFileChunk>>>,
    /// Parent (link) nodes re-constructed in memory
    links: HashMap<Hash, Arc<Node>>,
    #[serde(default)]
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    root_index: HashMap<Hash, Item>,
    #[serde(default)]
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    dirty: bool,
    #[serde(default)]
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    generation: u64,
}

fn relative_state_path(root: &Path, path: &Path) -> Result<PathBuf, Error> {
    let resolved = if path.is_absolute() && !path.starts_with(root) {
        let canonical_root = fs::canonicalize(root)?;
        let canonical_path = fs::canonicalize(path)?;
        let relative = canonical_path.strip_prefix(&canonical_root).map_err(|_| {
            invalid_state(format!(
                "path {} is outside the configured root",
                path.display()
            ))
        })?;
        root.join(crate::utils::path::validate_relative(relative)?)
    } else {
        crate::utils::path::resolve_beneath(root, path)?
    };
    resolved
        .strip_prefix(root)
        .map(Path::to_path_buf)
        .map_err(|_| {
            invalid_state(format!(
                "path {} is outside the configured root",
                path.display()
            ))
        })
}

fn validate_item_paths(root: &Path, item: &mut Item) -> Result<(), Error> {
    item.metadata.path = relative_state_path(root, &item.metadata.path)?;
    for entry in &item.manifest.entries {
        crate::utils::path::validate_relative(Path::new(&entry.relative_path))?;
    }
    Ok(())
}

fn validate_existing_chunk_path(
    root: &Path,
    relative_path: &Path,
    populated: bool,
    offset: u64,
    size: u64,
) -> Result<PathBuf, Error> {
    let relative_path = crate::utils::path::validate_relative(relative_path)?;
    let path = root.join(&relative_path);
    let mut current = root.to_path_buf();
    for component in relative_path.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current)?;
        if metadata.file_type().is_symlink() {
            return Err(invalid_state(format!(
                "chunk path {} contains a symbolic link",
                relative_path.display()
            )));
        }
    }
    let metadata = fs::metadata(&path)?;
    if !metadata.is_file() {
        return Err(invalid_state(format!(
            "chunk path {} is not a regular file",
            relative_path.display()
        )));
    }
    let canonical_root = fs::canonicalize(root)?;
    let canonical_path = fs::canonicalize(&path)?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(invalid_state(format!(
            "chunk path {} resolves outside the configured root",
            relative_path.display()
        )));
    }
    let end = offset
        .checked_add(size)
        .ok_or_else(|| invalid_state("chunk byte range overflows u64"))?;
    if populated && end > metadata.len() {
        return Err(invalid_state(format!(
            "populated chunk range {offset}..{end} exceeds {} bytes in {}",
            metadata.len(),
            relative_path.display()
        )));
    }
    Ok(path)
}

fn hash_file_range(path: &Path, offset: u64, size: u64) -> Result<Hash, Error> {
    let mut file = File::open(path)?;
    file.seek(std::io::SeekFrom::Start(offset))?;
    let size = usize::try_from(size).map_err(InvalidParameter::from)?;
    let mut bytes = vec![0; size];
    file.read_exact(&mut bytes)?;
    Ok(do_hash(&bytes))
}

fn verify_populated_chunk_hashes(data: &HashMap<Hash, Vec<Arc<InFileChunk>>>) -> Result<(), Error> {
    for (hash, entries) in data {
        for entry in entries {
            if entry.populated.load(std::sync::atomic::Ordering::Relaxed)
                && hash_file_range(&entry.path, entry.offset, entry.info.size)? != *hash
            {
                return Err(invalid_state(format!(
                    "populated bytes in {} do not match chunk hash {hash}",
                    entry.path.display()
                )));
            }
        }
    }
    Ok(())
}

fn recompute_item_root(chunks: &[ChunkInfo]) -> Result<ChunkInfo, Error> {
    if chunks.is_empty() {
        return Err(invalid_state("item chunk list is empty"));
    }
    let mut partials = chunks.to_vec();
    while partials.len() > 1 {
        let n = partials.len();
        for (to, index) in (0..n - 1).step_by(2).enumerate() {
            partials[to] = ChunkInfo {
                hash: merge_hashes(&partials[index].hash, &partials[index + 1].hash),
                size: partials[index]
                    .size
                    .checked_add(partials[index + 1].size)
                    .ok_or_else(|| invalid_state("item root size overflows u64"))?,
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

fn validate_retained_items(
    items: &HashSet<Item>,
    data: &HashMap<Hash, Vec<Arc<InFileChunk>>>,
) -> Result<(), Error> {
    let mut revision_identities = HashSet::default();
    for item in items {
        if item.metadata.artifact_id != item.manifest.artifact_id {
            return Err(invalid_state(
                "item metadata and manifest artifact IDs differ",
            ));
        }
        if u64::from(item.metadata.revision) != item.manifest.version {
            return Err(invalid_state("item metadata and manifest revisions differ"));
        }
        if !revision_identities.insert((item.metadata.artifact_id.clone(), item.manifest.version)) {
            return Err(invalid_state(
                "duplicate artifact ID and manifest version identity",
            ));
        }
        let chunk_count = u32::try_from(item.chunks.len())
            .map_err(|_| invalid_state("item has too many chunks"))?;
        if chunk_count == 0 || item.manifest.chunk_count != chunk_count {
            return Err(invalid_state("item manifest chunk count is inconsistent"));
        }
        if item.manifest.chunk_size != item.manifest.chunk_algorithm.nominal_chunk_size() {
            return Err(invalid_state(
                "item manifest nominal chunk size is inconsistent",
            ));
        }
        let root = recompute_item_root(&item.chunks)?;
        if item.metadata.root != root
            || item.manifest.root_hash != root.hash
            || item.manifest.total_size != root.size
        {
            return Err(invalid_state("item root or total size is inconsistent"));
        }

        let mut cursor = 0usize;
        let mut previous_path: Option<PathBuf> = None;
        let mut normalized_paths = HashSet::default();
        for entry in &item.manifest.entries {
            let normalized =
                crate::utils::path::validate_relative(Path::new(&entry.relative_path))?;
            if normalized.to_str() != Some(entry.relative_path.as_str()) {
                return Err(invalid_state(
                    "item entry path is not in canonical relative form",
                ));
            }
            if normalized
                .components()
                .next()
                .is_some_and(|component| component.as_os_str() == FS_STORAGE_REVISION_NAMESPACE)
            {
                return Err(invalid_state(
                    "item entry path uses the reserved revision namespace",
                ));
            }
            if !normalized_paths.insert(normalized.clone()) {
                return Err(invalid_state(
                    "item entry paths alias after canonical normalization",
                ));
            }
            if previous_path
                .as_ref()
                .is_some_and(|previous| previous >= &normalized)
            {
                return Err(invalid_state(
                    "item entry paths are duplicated or not strictly ordered",
                ));
            }
            previous_path = Some(normalized);
            let start = usize::try_from(entry.chunk_range.0).map_err(InvalidParameter::from)?;
            let end = usize::try_from(entry.chunk_range.1).map_err(InvalidParameter::from)?;
            if start != cursor || end <= start || end > item.chunks.len() {
                return Err(invalid_state("item entry chunk ranges are inconsistent"));
            }
            let size = item.chunks[start..end]
                .iter()
                .try_fold(0_u64, |total, chunk| total.checked_add(chunk.size))
                .ok_or_else(|| invalid_state("item entry size overflows u64"))?;
            if size != entry.size {
                return Err(invalid_state("item entry size is inconsistent"));
            }
            cursor = end;
        }
        if !item.manifest.entries.is_empty() && cursor != item.chunks.len() {
            return Err(invalid_state("item entries do not cover every chunk"));
        }

        for chunk in &item.chunks {
            let source = data.get(&chunk.hash).and_then(|entries| {
                entries.iter().find(|entry| {
                    entry.info == *chunk
                        && entry.populated.load(std::sync::atomic::Ordering::Relaxed)
                })
            });
            if source.is_none() {
                return Err(invalid_state(format!(
                    "item references unavailable populated chunk {}",
                    chunk.hash
                )));
            }
        }
    }
    Ok(())
}

fn validate_link_node(
    node: &Node,
    data: &HashMap<Hash, Vec<Arc<InFileChunk>>>,
    links: &HashMap<Hash, Arc<Node>>,
) -> Result<(), Error> {
    let mut stack = vec![(node, 0usize)];
    while let Some((node, depth)) = stack.pop() {
        if depth > 256 {
            return Err(invalid_state(
                "link tree exceeds the maximum supported depth",
            ));
        }
        match node {
            Node::Parent {
                hash,
                size,
                left,
                right,
            } => {
                let expected_size = left
                    .size()
                    .checked_add(right.size())
                    .ok_or_else(|| invalid_state("link node size overflows u64"))?;
                if *size != expected_size || *hash != merge_hashes(left.hash(), right.hash()) {
                    return Err(invalid_state("link node hash or size is inconsistent"));
                }
                stack.push((right, depth + 1));
                stack.push((left, depth + 1));
            }
            Node::Skipped { hash, size } => {
                let has_chunk = data.get(hash).is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        entry.info.size == *size
                            && entry.populated.load(std::sync::atomic::Ordering::Relaxed)
                    })
                });
                let has_link = links.get(hash).is_some_and(|linked| linked.size() == *size);
                if !has_chunk && !has_link {
                    return Err(invalid_state(format!(
                        "link leaf {hash} has no populated chunk or parent of size {size}"
                    )));
                }
            }
            Node::Stored { .. } => {
                let hash = *node.hash();
                let size = node.size();
                let has_chunk = data.get(&hash).is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        entry.info.size == size
                            && entry.populated.load(std::sync::atomic::Ordering::Relaxed)
                    })
                });
                if !has_chunk {
                    return Err(invalid_state(
                        "persisted link leaf has no matching populated chunk",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn relink_state_nodes(state: &mut FsStorageState) -> Result<(), Error> {
    fn relink_hash(
        hash: Hash,
        expected_size: u64,
        data: &HashMap<Hash, Vec<Arc<InFileChunk>>>,
        raw_links: &HashMap<Hash, Arc<Node>>,
        memo: &mut HashMap<Hash, Arc<Node>>,
        visiting: &mut HashSet<Hash>,
        depth: usize,
    ) -> Result<Arc<Node>, Error> {
        if let Some(node) = memo.get(&hash) {
            if node.size() != expected_size {
                return Err(invalid_state("reused link size is inconsistent"));
            }
            return Ok(node.clone());
        }
        if let Some(entry) = data.get(&hash).and_then(|entries| {
            entries.iter().find(|entry| {
                entry.info.size == expected_size
                    && entry.populated.load(std::sync::atomic::Ordering::Relaxed)
            })
        }) {
            return Node::try_from(entry).map(Arc::new);
        }
        if !visiting.insert(hash) {
            return Err(invalid_state("link graph contains a cycle"));
        }
        let raw = raw_links
            .get(&hash)
            .ok_or_else(|| invalid_state(format!("cannot reconstruct link leaf {hash}")))?;
        if raw.size() != expected_size {
            return Err(invalid_state("referenced link size is inconsistent"));
        }
        let rebuilt = relink_node(raw, data, raw_links, memo, visiting, depth + 1)?;
        visiting.remove(&hash);
        memo.insert(hash, rebuilt.clone());
        Ok(rebuilt)
    }

    fn relink_node(
        node: &Node,
        data: &HashMap<Hash, Vec<Arc<InFileChunk>>>,
        raw_links: &HashMap<Hash, Arc<Node>>,
        memo: &mut HashMap<Hash, Arc<Node>>,
        visiting: &mut HashSet<Hash>,
        depth: usize,
    ) -> Result<Arc<Node>, Error> {
        if depth > 256 {
            return Err(invalid_state(
                "link tree exceeds the maximum supported depth",
            ));
        }
        match node {
            Node::Parent {
                hash,
                size,
                left,
                right,
            } => Ok(Arc::new(Node::Parent {
                hash: *hash,
                size: *size,
                left: relink_node(left, data, raw_links, memo, visiting, depth + 1)?,
                right: relink_node(right, data, raw_links, memo, visiting, depth + 1)?,
            })),
            Node::Skipped { hash, size } => {
                relink_hash(*hash, *size, data, raw_links, memo, visiting, depth)
            }
            Node::Stored { hash, data: bytes } => relink_hash(
                *hash,
                bytes.len() as u64,
                data,
                raw_links,
                memo,
                visiting,
                depth,
            ),
        }
    }

    let old_links = std::mem::take(&mut state.links);
    let mut links = HashMap::default();
    let mut visiting = HashSet::default();
    for (hash, node) in &old_links {
        let rebuilt = relink_hash(
            *hash,
            node.size(),
            &state.data,
            &old_links,
            &mut links,
            &mut visiting,
            0,
        )?;
        if rebuilt.hash() != hash {
            return Err(invalid_state("link map key does not match node hash"));
        }
    }
    state.links = links;
    Ok(())
}

fn push_flat_link_record(
    records: &mut Vec<FsStorageNodeV1>,
    record: FsStorageNodeV1,
) -> Result<u32, Error> {
    if records.len() >= FS_STORAGE_MAX_LINK_RECORDS {
        return Err(invalid_state(
            "link graph exceeds the persisted record bound",
        ));
    }
    let index = u32::try_from(records.len()).map_err(InvalidParameter::from)?;
    records.push(record);
    Ok(index)
}

fn flatten_link_node(node: &Node) -> Result<Vec<FsStorageNodeV1>, Error> {
    fn visit(node: &Node, records: &mut Vec<FsStorageNodeV1>, depth: usize) -> Result<u32, Error> {
        if depth > FS_STORAGE_MAX_LINK_DEPTH {
            return Err(invalid_state("link graph exceeds the persisted bounds"));
        }
        match node {
            Node::Parent {
                hash,
                size,
                left,
                right,
            } => {
                let left = visit(left, records, depth + 1)?;
                let right = visit(right, records, depth + 1)?;
                push_flat_link_record(
                    records,
                    FsStorageNodeV1::Parent {
                        hash: *hash,
                        size: *size,
                        left,
                        right,
                    },
                )
            }
            Node::Skipped { hash, size } => push_flat_link_record(
                records,
                FsStorageNodeV1::Skipped {
                    hash: *hash,
                    size: *size,
                },
            ),
            Node::Stored { hash, data } => push_flat_link_record(
                records,
                FsStorageNodeV1::Skipped {
                    hash: *hash,
                    size: data.len() as u64,
                },
            ),
        }
    }

    let mut records = Vec::new();
    visit(node, &mut records, 0)?;
    Ok(records)
}

fn validate_flat_link(records: &[FsStorageNodeV1]) -> Result<(), Error> {
    if records.is_empty() || records.len() > FS_STORAGE_MAX_LINK_RECORDS {
        return Err(invalid_state("flat link record count is invalid"));
    }

    let mut identities = Vec::with_capacity(records.len());
    let mut depths = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        match record {
            FsStorageNodeV1::Skipped { hash, size } => {
                identities.push((*hash, *size));
                depths.push(0usize);
            }
            FsStorageNodeV1::Parent {
                hash,
                size,
                left,
                right,
            } => {
                let left = *left as usize;
                let right = *right as usize;
                if left >= index || right >= index {
                    return Err(invalid_state(
                        "flat link parent indices must refer to earlier records",
                    ));
                }
                let (left_hash, left_size) = identities[left];
                let (right_hash, right_size) = identities[right];
                let expected_size = left_size
                    .checked_add(right_size)
                    .ok_or_else(|| invalid_state("flat link size overflows"))?;
                if *size != expected_size || *hash != merge_hashes(&left_hash, &right_hash) {
                    return Err(invalid_state("flat link hash or size is inconsistent"));
                }
                let depth = depths[left]
                    .max(depths[right])
                    .checked_add(1)
                    .ok_or_else(|| invalid_state("flat link depth overflows"))?;
                if depth > FS_STORAGE_MAX_LINK_DEPTH {
                    return Err(invalid_state("flat link exceeds maximum depth"));
                }
                identities.push((*hash, *size));
                depths.push(depth);
            }
        }
    }

    // Every record must participate in the final-root DAG. This rejects
    // otherwise-valid orphan records without constructing recursive Arcs.
    let mut reachable = vec![false; records.len()];
    let mut stack = vec![records.len() - 1];
    while let Some(index) = stack.pop() {
        if std::mem::replace(&mut reachable[index], true) {
            continue;
        }
        if let FsStorageNodeV1::Parent { left, right, .. } = &records[index] {
            stack.push(*left as usize);
            stack.push(*right as usize);
        }
    }
    if reachable.iter().any(|reachable| !reachable) {
        return Err(invalid_state(
            "flat link contains records outside the root DAG",
        ));
    }
    Ok(())
}

fn materialize_flat_link(records: Vec<FsStorageNodeV1>) -> Result<Arc<Node>, Error> {
    validate_flat_link(&records)?;
    let mut nodes: Vec<Arc<Node>> = Vec::with_capacity(records.len());
    for record in records {
        let node = match record {
            FsStorageNodeV1::Skipped { hash, size } => Arc::new(Node::Skipped { hash, size }),
            FsStorageNodeV1::Parent {
                hash,
                size,
                left,
                right,
            } => {
                let left = nodes
                    .get(left as usize)
                    .ok_or_else(|| invalid_state("flat link left index is not earlier"))?
                    .clone();
                let right = nodes
                    .get(right as usize)
                    .ok_or_else(|| invalid_state("flat link right index is not earlier"))?
                    .clone();
                let expected_size = left
                    .size()
                    .checked_add(right.size())
                    .ok_or_else(|| invalid_state("flat link size overflows"))?;
                if size != expected_size || hash != merge_hashes(left.hash(), right.hash()) {
                    return Err(invalid_state("flat link hash or size is inconsistent"));
                }
                Arc::new(Node::Parent {
                    hash,
                    size,
                    left,
                    right,
                })
            }
        };
        nodes.push(node);
    }
    nodes
        .pop()
        .ok_or_else(|| invalid_state("flat link has no root"))
}

impl FsStorageStateV1 {
    fn from_runtime(state: &FsStorageState) -> Result<Self, Error> {
        let root_identity = fs_storage_root_identity(&state.root)?;

        let mut items = state.items.iter().cloned().collect::<Vec<_>>();
        for item in &mut items {
            validate_item_paths(&state.root, item)?;
        }
        sort_by_bitcode_identity(&mut items)?;

        let mut data = Vec::with_capacity(state.data.len());
        for (hash, entries) in &state.data {
            let mut chunks = Vec::with_capacity(entries.len());
            for entry in entries {
                if entry.info.hash != *hash {
                    return Err(invalid_state("chunk map key does not match chunk hash"));
                }
                let relative_path = relative_state_path(&state.root, &entry.path)?;
                chunks.push(FsStorageChunkV1 {
                    info: entry.info,
                    relative_path,
                    offset: entry.offset,
                    populated: entry.populated.load(std::sync::atomic::Ordering::Relaxed),
                });
            }
            sort_by_bitcode_identity(&mut chunks)?;
            data.push(FsStorageDataEntryV1 {
                hash: *hash,
                chunks,
            });
        }
        sort_by_bitcode_identity(&mut data)?;

        let mut links = state
            .links
            .iter()
            .map(|(hash, node)| {
                Ok(FsStorageLinkEntryV1 {
                    hash: *hash,
                    nodes: flatten_link_node(node)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        for entry in &links {
            let node = materialize_flat_link(entry.nodes.clone())?;
            if node.hash() != &entry.hash {
                return Err(invalid_state("link map key does not match node hash"));
            }
            validate_link_node(&node, &state.data, &state.links)?;
        }
        sort_by_bitcode_identity(&mut links)?;

        Ok(Self {
            root_identity,
            items,
            data,
            links,
        })
    }

    fn into_runtime(
        mut self,
        root: PathBuf,
        persistance_path: PathBuf,
    ) -> Result<FsStorageState, Error> {
        if self.root_identity != fs_storage_root_identity(&root)? {
            return Err(invalid_state(
                "root identity does not match constructor root",
            ));
        }
        require_canonical_order(&self.items)?;
        require_canonical_order(&self.data)?;
        require_canonical_order(&self.links)?;

        let mut items = HashSet::default();
        for mut item in self.items.drain(..) {
            validate_item_paths(&root, &mut item)?;
            if !items.insert(item) {
                return Err(invalid_state("duplicate item"));
            }
        }

        let mut data = HashMap::default();
        for entry in self.data {
            require_canonical_order(&entry.chunks)?;
            if entry.chunks.is_empty() {
                return Err(invalid_state("chunk map entry is empty"));
            }
            let mut chunks = Vec::with_capacity(entry.chunks.len());
            for chunk in entry.chunks {
                if chunk.info.hash != entry.hash {
                    return Err(invalid_state("chunk map key does not match chunk hash"));
                }
                let path = validate_existing_chunk_path(
                    &root,
                    &chunk.relative_path,
                    chunk.populated,
                    chunk.offset,
                    chunk.info.size,
                )?;
                chunks.push(Arc::new(InFileChunk {
                    info: chunk.info,
                    path,
                    offset: chunk.offset,
                    populated: Arc::new(AtomicBool::new(chunk.populated)),
                }));
            }
            if data.insert(entry.hash, chunks).is_some() {
                return Err(invalid_state("duplicate chunk-map hash"));
            }
        }
        verify_populated_chunk_hashes(&data)?;
        validate_retained_items(&items, &data)?;

        let mut links = HashMap::default();
        for entry in self.links {
            let node = materialize_flat_link(entry.nodes)?;
            if node.hash() != &entry.hash {
                return Err(invalid_state("link map key does not match node hash"));
            }
            if links.insert(entry.hash, node).is_some() {
                return Err(invalid_state("duplicate link-map hash"));
            }
        }
        for node in links.values() {
            validate_link_node(node, &data, &links)?;
        }

        let mut state = FsStorageState {
            root,
            items,
            persistance_path,
            data,
            links,
            root_index: HashMap::default(),
            dirty: false,
            generation: 0,
        };
        relink_state_nodes(&mut state)?;
        state.rebuild_indexes();
        Ok(state)
    }
}

impl LegacyFsStorageState {
    fn into_v1(self, root: &Path, expected_legacy_path: &Path) -> Result<FsStorageStateV1, Error> {
        let legacy_root = fs::canonicalize(&self.root)?;
        let constructor_root = fs::canonicalize(root)?;
        if legacy_root != constructor_root {
            return Err(invalid_state(format!(
                "legacy root {} does not match constructor root {}",
                self.root.display(),
                root.display()
            )));
        }
        if self.persistance_path != expected_legacy_path {
            return Err(invalid_state(format!(
                "legacy persistence path {} does not match its source {}",
                self.persistance_path.display(),
                expected_legacy_path.display()
            )));
        }

        for entries in self.data.values() {
            for entry in entries {
                if !entry.path.is_absolute() {
                    return Err(invalid_state(format!(
                        "legacy chunk path {} is not absolute",
                        entry.path.display()
                    )));
                }
                let relative = relative_state_path(root, &entry.path)?;
                validate_existing_chunk_path(
                    root,
                    &relative,
                    entry.populated.load(std::sync::atomic::Ordering::Relaxed),
                    entry.offset,
                    entry.info.size,
                )?;
            }
        }

        let runtime = FsStorageState {
            root: root.to_path_buf(),
            items: self.items,
            persistance_path: expected_legacy_path.to_path_buf(),
            data: self.data,
            links: self.links,
            root_index: HashMap::default(),
            dirty: false,
            generation: 0,
        };
        verify_populated_chunk_hashes(&runtime.data)?;
        validate_retained_items(&runtime.items, &runtime.data)?;
        FsStorageStateV1::from_runtime(&runtime)
    }
}

impl FsStorageState {
    fn mark_dirty(&mut self) {
        self.dirty = true;
        self.generation = self.generation.wrapping_add(1);
    }

    fn rebuild_indexes(&mut self) {
        self.root_index.clear();
        for item in &self.items {
            let root_hash = item.metadata.root.hash;
            let replace = self
                .root_index
                .get(&root_hash)
                .map(|current| {
                    (item.metadata.revision, item.chunks.len(), item.size())
                        > (
                            current.metadata.revision,
                            current.chunks.len(),
                            current.size(),
                        )
                })
                .unwrap_or(true);
            if replace {
                self.root_index.insert(root_hash, item.clone());
            }
        }
    }

    fn insert_item(&mut self, item: Item) {
        self.items.replace(item);
        self.rebuild_indexes();
        self.mark_dirty();
    }

    fn remove_item(&mut self, item: &Item) -> bool {
        let removed = self.items.remove(item);
        if removed {
            self.rebuild_indexes();
            self.mark_dirty();
        }
        removed
    }

    fn retain_links_reachable_from_items(&mut self) {
        fn visit(
            node: &Node,
            links: &HashMap<Hash, Arc<Node>>,
            retained: &mut HashSet<Hash>,
            depth: usize,
        ) {
            if depth > 256 {
                return;
            }
            match node {
                Node::Parent { left, right, .. } => {
                    if !retained.insert(*node.hash()) {
                        return;
                    }
                    visit(left, links, retained, depth + 1);
                    visit(right, links, retained, depth + 1);
                }
                Node::Skipped { hash, .. } => {
                    if retained.insert(*hash) {
                        if let Some(Node::Parent { left, right, .. }) =
                            links.get(hash).map(Arc::as_ref)
                        {
                            visit(left, links, retained, depth + 1);
                            visit(right, links, retained, depth + 1);
                        }
                    }
                }
                Node::Stored { .. } => {
                    retained.insert(*node.hash());
                }
            }
        }

        let mut retained = HashSet::default();
        for item in &self.items {
            if let Some(root) = self.links.get(&item.metadata.root.hash) {
                visit(root, &self.links, &mut retained, 0);
            }
        }
        self.links.retain(|hash, _| retained.contains(hash));
    }

    /// Returns the stored path of any path relative to root
    fn path(&self, path: &Path) -> Result<PathBuf, Error> {
        let resolved = crate::utils::path::resolve_beneath(&self.root, path)?;
        let relative = resolved
            .strip_prefix(&self.root)
            .map_err(|_| Error::MissingData)?;
        let mut current = self.root.clone();
        for component in relative.components() {
            current.push(component.as_os_str());
            match std::fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(InvalidParameter::Generic {
                        expected: "a destination without symbolic-link components".to_string(),
                        got: path.to_string_lossy().into_owned(),
                    }
                    .into());
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(resolved)
    }

    /// Full on-disk path for an item
    fn item_path(&self, item: &Item) -> Result<PathBuf, Error> {
        let full_path = self.path(&item.metadata.path)?;
        create_dir_all(full_path.parent().unwrap_or(&full_path))?;
        tracing::debug!(
            "Created path {:?}",
            full_path.parent().unwrap_or(&full_path)
        );
        Ok(full_path)
    }

    fn item_for_root(&self, root: &Hash) -> Option<Item> {
        self.root_index.get(root).cloned()
    }

    /// Pre-allocate a single `ChunkInfo` slot on disk
    fn pre_allocate_chunk(
        &mut self,
        path: &Path,
        chunk_info: &ChunkInfo,
        offset: u64,
    ) -> Result<(), Error> {
        // If already registered for this path/offset, skip
        if let Some(ifcs) = self.data.get(&chunk_info.hash) {
            for ifc in ifcs {
                if path == ifc.path && offset == ifc.offset {
                    return Ok(());
                }
            }
        }

        let ifc = InFileChunk {
            info: *chunk_info,
            path: path.to_owned(),
            offset,
            populated: Arc::default(),
        };
        tracing::trace!("Created infile chunk: {ifc:?}");
        self.data
            .entry(chunk_info.hash)
            .or_default()
            .push(Arc::new(ifc));
        self.mark_dirty();
        Ok(())
    }

    /// Pre-allocate space for multiple `ChunkInfo` slots on disk
    fn pre_allocate(&mut self, path: &Path, data: &[ChunkInfo]) -> Result<(), Error> {
        tracing::debug!(
            "Preallocating {} chunks at {path:?}, for a total of {} bytes",
            data.len(),
            data.iter().map(|x| x.size).sum::<u64>()
        );
        let mut offset = 0;
        for chunk in data {
            tracing::trace!(
                "Preallocating {}, {} bytes, {} offset",
                chunk.hash,
                chunk.size,
                offset
            );
            self.pre_allocate_chunk(path, chunk, offset)?;
            offset += chunk.size;
        }
        Ok(())
    }

    /// Pre-allocate space for raw bytes on disk
    fn pre_allocate_bytes(&mut self, path: &Path, data: &[u8]) -> Result<(), Error> {
        tracing::debug!("Preallocating {} bytes at {path:?}", data.len());
        let chunks: Vec<ChunkInfo> = data
            .chunks(CHUNK_SIZE)
            .map(|chunk| ChunkInfo {
                hash: do_hash(chunk),
                size: chunk.len() as u64,
            })
            .collect();
        self.pre_allocate(path, &chunks)
    }

    /// Store a parent (link) node
    fn store_link(
        &mut self,
        hash: Hash,
        left: Arc<Node>,
        right: Arc<Node>,
    ) -> Result<Arc<Node>, StorageError> {
        let size = left
            .size()
            .checked_add(right.size())
            .ok_or(StorageError::ImmutableConflict)?;
        if merge_hashes(left.hash(), right.hash()) != hash {
            return Err(StorageError::ImmutableConflict);
        }
        let expected = Arc::new(Node::Parent {
            hash,
            left,
            right,
            size,
        });
        if let Some(existing) = self.links.get(&hash) {
            if existing != &expected {
                return Err(StorageError::ImmutableConflict);
            }
            return Ok(existing.clone());
        }
        self.links.insert(hash, expected.clone());
        self.mark_dirty();
        Ok(expected)
    }

    fn store_compact_link(
        &mut self,
        hash: Hash,
        left: Arc<Node>,
        right: Arc<Node>,
    ) -> Result<Arc<Node>, StorageError> {
        let size = left.size() + right.size();
        let res = self
            .links
            .entry(hash)
            .or_insert_with(|| {
                Arc::new(Node::Parent {
                    hash,
                    left: Arc::new(Node::Skipped {
                        hash: *left.hash(),
                        size: left.size(),
                    }),
                    right: Arc::new(Node::Skipped {
                        hash: *right.hash(),
                        size: right.size(),
                    }),
                    size,
                })
            })
            .clone();
        self.mark_dirty();
        Ok(res)
    }
}

// ─── Public handle ────────────────────────────────────────────────────────────

/// Storage keeping files in the filesystem instead of stored chunks independently.
///
/// It is useful to actually install files in the filesystem if the root is set to `/`.
///
/// Most logic is implemented in [`FsStorageState`]; this type is an `RwLock`-guarded handle
/// providing interior mutability so that `ChunkStorage` methods can take `&self`.
#[derive(Debug)]
pub struct FsStorage {
    inner: RwLock<FsStorageState>,
    persistence_lock: Mutex<()>,
    handles: RwLock<HashMap<PathBuf, Arc<Mutex<Handle>>>>,
    chunk_cache: Mutex<HotChunkCache>,
    tree_cache: Mutex<HotNodeCache>,
}

impl Default for FsStorage {
    fn default() -> Self {
        Self {
            inner: RwLock::new(FsStorageState::default()),
            persistence_lock: Mutex::new(()),
            handles: RwLock::new(HashMap::default()),
            chunk_cache: Mutex::new(HotChunkCache::new(DEFAULT_CHUNK_CACHE_BYTES)),
            tree_cache: Mutex::new(HotNodeCache::new(DEFAULT_TREE_CACHE_ENTRIES)),
        }
    }
}

impl FsStorage {
    fn artifact_entry_path(root: &Path, relative_path: &Path) -> Result<PathBuf, Error> {
        crate::utils::path::resolve_beneath(root, relative_path)
    }

    fn revision_chunk_path(&self, hash: &Hash) -> Result<PathBuf, Error> {
        self.path(
            &Path::new(FS_STORAGE_REVISION_NAMESPACE)
                .join("chunks")
                .join(format!("{hash}.chunk")),
        )
    }

    fn verify_cas_chunk(path: &Path, expected: &ChunkInfo) -> Result<(), Error> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid_state(format!(
                "immutable chunk path {} is not a regular file",
                path.display()
            )));
        }
        if metadata.len() != expected.size
            || hash_file_range(path, 0, expected.size)? != expected.hash
        {
            return Err(invalid_state(format!(
                "immutable chunk path {} conflicts with hash {}",
                path.display(),
                expected.hash
            )));
        }
        Ok(())
    }

    fn commit_cas_chunk(&self, expected: ChunkInfo, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() as u64 != expected.size || do_hash(bytes) != expected.hash {
            return Err(invalid_state(
                "supplied revision bytes do not match prepared chunk metadata",
            ));
        }
        let path = self.revision_chunk_path(&expected.hash)?;
        let parent = path.parent().ok_or(Error::MissingData)?;
        create_dir_all(parent)?;
        match fs::symlink_metadata(&path) {
            Ok(_) => Self::verify_cas_chunk(&path, &expected)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let staging =
                    parent.join(format!(".{}.{}.tmp", expected.hash, uuid::Uuid::new_v4()));
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&staging)?;
                if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
                    drop(file);
                    let _ = remove_file(&staging);
                    return Err(error.into());
                }
                drop(file);
                match fs::hard_link(&staging, &path) {
                    Ok(()) => {
                        remove_file(&staging)?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        remove_file(&staging)?;
                        Self::verify_cas_chunk(&path, &expected)?;
                    }
                    Err(error) => {
                        let _ = remove_file(&staging);
                        return Err(error.into());
                    }
                }
                #[cfg(unix)]
                File::open(parent)?.sync_all()?;
            }
            Err(error) => return Err(error.into()),
        }

        let mut inner = self.inner.write().unwrap();
        let locations = inner.data.entry(expected.hash).or_default();
        if !locations.iter().any(|entry| entry.path == path) {
            locations.insert(
                0,
                Arc::new(InFileChunk {
                    info: expected,
                    path,
                    offset: 0,
                    populated: Arc::new(AtomicBool::new(true)),
                }),
            );
            inner.mark_dirty();
        }
        Ok(())
    }

    fn finish_cas_revision(&self, item: Item) -> Result<Item, Error> {
        let partials = item
            .chunks
            .iter()
            .map(|chunk| {
                Arc::new(Node::Skipped {
                    hash: chunk.hash,
                    size: chunk.size,
                })
            })
            .collect();
        let root = self.finalize_sync_partials(partials)?;
        if root.chunk_info() != item.metadata.root {
            return Err(invalid_state(
                "prepared revision root does not match its chunk sequence",
            ));
        }
        self.inner.write().unwrap().insert_item(item.clone());
        self.persist()?;
        Ok(item)
    }

    fn migrate_referenced_chunks_to_cas(&self) -> Result<bool, Error> {
        let referenced = self
            .inner
            .read()
            .unwrap()
            .items
            .iter()
            .flat_map(|item| item.chunks.iter().copied())
            .collect::<HashSet<_>>();
        if referenced.is_empty() {
            return Ok(false);
        }

        let mut migrated = false;
        for chunk in &referenced {
            let cas_path = self.revision_chunk_path(&chunk.hash)?;
            let source = {
                let inner = self.inner.read().unwrap();
                inner.data.get(&chunk.hash).and_then(|entries| {
                    entries
                        .iter()
                        .find(|entry| {
                            entry.info == *chunk
                                && entry.populated.load(std::sync::atomic::Ordering::Relaxed)
                        })
                        .cloned()
                })
            };
            let source = source.ok_or_else(|| {
                invalid_state(format!(
                    "item references unavailable populated chunk {}",
                    chunk.hash
                ))
            })?;
            if source.path == cas_path && source.offset == 0 {
                Self::verify_cas_chunk(&cas_path, chunk)?;
                continue;
            }
            let size = usize::try_from(chunk.size).map_err(InvalidParameter::from)?;
            let mut bytes = vec![0; size];
            let mut file = File::open(&source.path)?;
            file.seek(std::io::SeekFrom::Start(source.offset))?;
            file.read_exact(&mut bytes)?;
            self.commit_cas_chunk(*chunk, &bytes)?;
            migrated = true;
        }

        if migrated {
            let mut inner = self.inner.write().unwrap();
            for chunk in &referenced {
                let cas_path = inner.path(
                    &Path::new(FS_STORAGE_REVISION_NAMESPACE)
                        .join("chunks")
                        .join(format!("{}.chunk", chunk.hash)),
                )?;
                if let Some(entries) = inner.data.get_mut(&chunk.hash) {
                    if let Some(index) = entries
                        .iter()
                        .position(|entry| entry.path == cas_path && entry.offset == 0)
                    {
                        let cas_entry = entries.remove(index);
                        entries.insert(0, cas_entry);
                    }
                }
            }
            inner.mark_dirty();
        }
        Ok(migrated)
    }

    fn combine_sync_nodes(&self, left: Arc<Node>, right: Arc<Node>) -> Result<Arc<Node>, Error> {
        let hash = merge_hashes(left.hash(), right.hash());
        let size = left.size() + right.size();

        if matches!(left.as_ref(), Node::Skipped { .. })
            && matches!(right.as_ref(), Node::Skipped { .. })
            && self.contains_hash(&hash)
        {
            return Ok(Arc::new(Node::Skipped { hash, size }));
        }

        self.store_compact_link(hash, left, right)
            .map_err(Error::from)
    }

    fn finalize_sync_partials(&self, mut partials: Vec<Arc<Node>>) -> Result<Arc<Node>, Error> {
        if partials.is_empty() {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }

        while partials.len() > 1 {
            let n = partials.len();
            for (to, index) in (0..n - 1).step_by(2).enumerate() {
                partials[to] =
                    self.combine_sync_nodes(partials[index].clone(), partials[index + 1].clone())?;
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

    fn contains_hash(&self, hash: &Hash) -> bool {
        let inner = self.inner.read().unwrap();
        inner.links.contains_key(hash) || inner.data.contains_key(hash)
    }

    fn item_for_root_hash(&self, root: &Hash) -> Option<Item> {
        self.inner.read().unwrap().item_for_root(root)
    }

    fn cached_chunk_data(&self, hash: &Hash) -> Option<ChunkData> {
        self.chunk_cache.lock().unwrap().get(hash)
    }

    fn cached_tree_node(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.tree_cache.lock().unwrap().get(hash)
    }

    fn cache_chunk_data(&self, hash: Hash, data: Arc<Vec<u8>>) -> Arc<Vec<u8>> {
        self.chunk_cache
            .lock()
            .unwrap()
            .insert_owned(hash, data.clone());
        data
    }

    fn cache_verified_received_chunks(&self, received_in_order: Vec<(Hash, ReceivedChunk)>) {
        let mut cache = self.chunk_cache.lock().unwrap();
        for (hash, data) in received_in_order {
            cache.insert_shared(hash, data);
        }
    }

    fn cache_tree_node(&self, hash: Hash, node: Arc<Node>) -> Arc<Node> {
        self.tree_cache.lock().unwrap().insert(hash, node.clone());
        node
    }

    fn read_chunk_data(&self, hash: &Hash) -> Option<ChunkData> {
        if let Some(cached) = self.cached_chunk_data(hash) {
            return Some(cached);
        }

        let entry = {
            let inner = self.inner.read().unwrap();
            inner
                .data
                .get(hash)
                .and_then(|entries| entries.first())
                .cloned()
        }?;

        if !entry.populated.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }

        let size = usize::try_from(entry.info.size).ok()?;
        let mut buf = vec![0u8; size];

        // Try to read via the existing handle (avoids File::open per chunk)
        if let Some(handle) = self.handle_for_path(&entry.path) {
            if handle.lock().unwrap().read(&mut buf, entry.offset).is_ok() {
                return Some(ChunkData::Owned(
                    self.cache_chunk_data(*hash, Arc::new(buf)),
                ));
            }
        }

        // Fallback: open file directly (handle may not exist on server side)
        let mut file = File::open(&entry.path).ok()?;
        file.seek(std::io::SeekFrom::Start(entry.offset)).ok()?;
        file.read_exact(&mut buf).ok()?;
        Some(ChunkData::Owned(
            self.cache_chunk_data(*hash, Arc::new(buf)),
        ))
    }

    fn stored_node(&self, hash: &Hash) -> Option<Arc<Node>> {
        let data = self.read_chunk_data(hash)?.into_owned();
        Some(Arc::new(Node::Stored { hash: *hash, data }))
    }

    fn build_compact_tree(
        &self,
        data: &[u8],
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<(Arc<Node>, Vec<ChunkInfo>), Error> {
        let boundaries = chunk_algorithm.chunk_boundaries(data);
        let mut partials = Vec::with_capacity(boundaries.len().max(1));
        let mut chunks = Vec::with_capacity(boundaries.len().max(1));

        if data.is_empty() {
            let hash = do_hash(&[]);
            let chunk_info = ChunkInfo { hash, size: 0 };
            self.store_chunk(hash, &[]).map_err(Error::from)?;
            chunks.push(chunk_info);
            partials.push(Arc::new(Node::Skipped { hash, size: 0 }));
        } else {
            for (offset, length) in boundaries {
                let chunk = &data[offset..offset + length];
                let hash = do_hash(chunk);
                let chunk_info = ChunkInfo {
                    hash,
                    size: chunk.len() as u64,
                };
                self.store_chunk(hash, chunk).map_err(Error::from)?;
                chunks.push(chunk_info);
                partials.push(Arc::new(Node::Skipped {
                    hash,
                    size: chunk.len() as u64,
                }));
            }
        }

        let root = self.finalize_sync_partials(partials)?;
        Ok((root, chunks))
    }

    fn build_compact_tree_from_files(
        &self,
        root_path: &Path,
        files: &[(PathBuf, bytes::Bytes)],
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<(Arc<Node>, Vec<ChunkInfo>, Vec<FileEntry>), Error> {
        let mut partials = Vec::new();
        let mut chunks = Vec::new();
        let mut entries = Vec::with_capacity(files.len());
        let mut next_chunk_index = 0u32;

        for (relative_path, data) in files {
            let full_path = self.path(&Self::artifact_entry_path(root_path, relative_path)?)?;
            create_dir_all(full_path.parent().ok_or(Error::MissingData)?)?;

            let start = next_chunk_index;
            let mut offset = 0u64;
            let boundaries = if data.is_empty() {
                vec![(0, 0)]
            } else {
                chunk_algorithm.chunk_boundaries(data.as_ref())
            };
            for (chunk_offset, length) in boundaries {
                let chunk = &data.as_ref()[chunk_offset..chunk_offset + length];
                let hash = do_hash(chunk);
                let chunk_info = ChunkInfo {
                    hash,
                    size: chunk.len() as u64,
                };
                self.inner
                    .write()
                    .unwrap()
                    .pre_allocate_chunk(&full_path, &chunk_info, offset)?;
                self.store_chunk(hash, chunk).map_err(Error::from)?;
                chunks.push(chunk_info);
                partials.push(Arc::new(Node::Skipped {
                    hash,
                    size: chunk.len() as u64,
                }));
                offset += chunk.len() as u64;
                next_chunk_index += 1;
            }

            entries.push(FileEntry {
                relative_path: relative_path.to_string_lossy().to_string(),
                size: data.len() as u64,
                chunk_range: (start, next_chunk_index),
            });
        }

        if partials.is_empty() {
            return Err(Error::MissingData);
        }

        let root = self.finalize_sync_partials(partials)?;
        Ok((root, chunks, entries))
    }

    fn receive_manifest_entry_chunks(
        &self,
        manifest: &Manifest,
        chunk_infos: &[ChunkInfo],
        available_hashes: &HashSet<Hash>,
        received_chunks: &mut VecDeque<ReceivedChunk>,
        staged_data: &mut HashMap<Hash, ChunkData>,
        received_in_order: &mut Vec<(Hash, ReceivedChunk)>,
    ) -> Result<(Vec<ChunkInfo>, Arc<Node>), Error> {
        let mut partials = Vec::with_capacity(chunk_infos.len());
        let mut chunks = Vec::with_capacity(chunk_infos.len());

        for entry in &manifest.entries {
            let start = entry.chunk_range.0 as usize;
            let end = entry.chunk_range.1 as usize;
            for index in start..end {
                let chunk_info = *chunk_infos
                    .get(index)
                    .ok_or_else(|| Error::Storage(StorageError::TreeReconstruct))?;

                chunks.push(chunk_info);

                if available_hashes.contains(&chunk_info.hash) {
                    let data = self
                        .read_chunk_data(&chunk_info.hash)
                        .ok_or(Error::Storage(StorageError::TreeReconstruct))?;
                    staged_data.insert(chunk_info.hash, data);
                } else {
                    let data = received_chunks
                        .pop_front()
                        .ok_or_else(|| Error::Storage(StorageError::TreeReconstruct))?;
                    if do_hash(&data) != chunk_info.hash {
                        return Err(Error::Storage(StorageError::TreeReconstruct));
                    }
                    received_in_order.push((chunk_info.hash, data.clone()));
                    staged_data.insert(chunk_info.hash, ChunkData::Shared(data));
                }

                partials.push(Arc::new(Node::Skipped {
                    hash: chunk_info.hash,
                    size: chunk_info.size,
                }));
            }
        }

        let root = self.finalize_sync_partials(partials)?;
        Ok((chunks, root))
    }

    fn activate_staged_chunks(
        &self,
        item_root: &Path,
        manifest: &Manifest,
        chunks: &[ChunkInfo],
        staged_data: &HashMap<Hash, ChunkData>,
    ) -> Result<(), Error> {
        let parent = item_root.parent().ok_or(Error::MissingData)?;
        create_dir_all(parent)?;
        let item_name = item_root.file_name().ok_or(Error::MissingData)?;
        let staging_root = parent.join(format!(
            ".{}.distd-stage-{}",
            item_name.to_string_lossy(),
            uuid::Uuid::new_v4()
        ));

        let mut staged_files = Vec::new();
        let mut final_files = Vec::new();
        let mut ranges = Vec::new();

        if manifest.entries.is_empty() {
            staged_files.push(staging_root.clone());
            final_files.push(item_root.to_path_buf());
            ranges.push((0usize, chunks.len()));
        } else {
            create_dir_all(&staging_root)?;
            let mut expected_start = 0usize;
            let mut seen_paths = HashSet::default();
            for entry in &manifest.entries {
                let relative_path =
                    crate::utils::path::validate_relative(Path::new(&entry.relative_path))?;
                if !seen_paths.insert(relative_path.clone()) {
                    return Err(Error::Storage(StorageError::TreeReconstruct));
                }

                let start = entry.chunk_range.0 as usize;
                let end = entry.chunk_range.1 as usize;
                if start != expected_start || end < start || end > chunks.len() {
                    return Err(Error::Storage(StorageError::TreeReconstruct));
                }
                let actual_size = chunks[start..end]
                    .iter()
                    .map(|chunk| chunk.size)
                    .sum::<u64>();
                if actual_size != entry.size {
                    return Err(Error::Storage(StorageError::TreeReconstruct));
                }
                expected_start = end;

                staged_files.push(Self::artifact_entry_path(&staging_root, &relative_path)?);
                final_files.push(Self::artifact_entry_path(item_root, &relative_path)?);
                ranges.push((start, end));
            }
            if expected_start != chunks.len() {
                return Err(Error::Storage(StorageError::TreeReconstruct));
            }
        }

        let materialize_result = staged_files
            .iter()
            .zip(ranges.iter().copied())
            .try_for_each(|(path, (start, end))| {
                if let Some(parent) = path.parent() {
                    create_dir_all(parent)?;
                }
                let mut file = File::create(path)?;
                for chunk in &chunks[start..end] {
                    let data = staged_data
                        .get(&chunk.hash)
                        .ok_or(Error::Storage(StorageError::TreeReconstruct))?;
                    if data.as_ref().len() as u64 != chunk.size
                        || do_hash(data.as_ref()) != chunk.hash
                    {
                        return Err(Error::Storage(StorageError::TreeReconstruct));
                    }
                    file.write_all(data.as_ref())?;
                }
                file.set_len(chunks[start..end].iter().map(|chunk| chunk.size).sum())?;
                file.sync_all()?;
                Ok::<(), Error>(())
            });

        if let Err(error) = materialize_result {
            Self::remove_staging_path(&staging_root);
            return Err(error);
        }

        if let Err(error) = Self::atomic_replace(&staging_root, item_root) {
            Self::remove_staging_path(&staging_root);
            return Err(error);
        }

        let mut inner = self.inner.write().unwrap();
        for entries in inner.data.values_mut() {
            entries.retain(|entry| !entry.path.starts_with(item_root));
        }
        inner.data.retain(|_, entries| !entries.is_empty());
        for (path, (start, end)) in final_files.iter().zip(ranges.iter().copied()) {
            let mut offset = 0u64;
            for chunk in &chunks[start..end] {
                inner
                    .data
                    .entry(chunk.hash)
                    .or_default()
                    .push(Arc::new(InFileChunk {
                        info: *chunk,
                        path: path.clone(),
                        offset,
                        populated: Arc::new(AtomicBool::new(true)),
                    }));
                offset += chunk.size;
            }
        }
        inner.mark_dirty();
        drop(inner);

        let mut handles = self.handles.write().unwrap();
        handles.retain(|path, _| !path.starts_with(item_root));
        for path in final_files {
            handles.insert(path.clone(), Arc::new(Mutex::new(Handle::new(&path)?)));
        }
        Ok(())
    }

    fn remove_staging_path(path: &Path) {
        if path.is_dir() {
            let _ = fs::remove_dir_all(path);
        } else {
            let _ = fs::remove_file(path);
        }
    }

    fn atomic_replace(staging: &Path, destination: &Path) -> Result<(), Error> {
        let parent = destination.parent().ok_or(Error::MissingData)?;
        let name = destination.file_name().ok_or(Error::MissingData)?;
        let backup = parent.join(format!(
            ".{}.distd-backup-{}",
            name.to_string_lossy(),
            uuid::Uuid::new_v4()
        ));
        let had_destination = destination.exists();
        if had_destination {
            fs::rename(destination, &backup)?;
        }

        if let Err(error) = fs::rename(staging, destination) {
            if had_destination {
                let _ = fs::rename(&backup, destination);
            }
            return Err(error.into());
        }

        if had_destination {
            Self::remove_staging_path(&backup);
        }
        File::open(parent)?.sync_all()?;
        Ok(())
    }

    fn materialize_node(&self, node: Arc<Node>) -> Option<Arc<Node>> {
        if let Some(cached) = self.cached_tree_node(node.hash()) {
            return Some(cached);
        }

        match node.as_ref() {
            Node::Stored { .. } => Some(node),
            Node::Parent {
                hash,
                size,
                left,
                right,
            } => Some(self.cache_tree_node(
                *hash,
                Arc::new(Node::Parent {
                    hash: *hash,
                    size: *size,
                    left: self.materialize_node(left.clone())?,
                    right: self.materialize_node(right.clone())?,
                }),
            )),
            Node::Skipped { hash, .. } => {
                let linked = {
                    let inner = self.inner.read().unwrap();
                    inner.links.get(hash).cloned()
                };
                if let Some(linked) = linked {
                    self.materialize_node(linked)
                } else {
                    self.stored_node(hash)
                }
            }
        }
    }

    fn store_compact_link(
        &self,
        hash: Hash,
        left: Arc<Node>,
        right: Arc<Node>,
    ) -> Result<Arc<Node>, StorageError> {
        self.inner
            .write()
            .unwrap()
            .store_compact_link(hash, left, right)
    }

    fn try_fill_in_compact(&self, tree: &Node) -> Result<Arc<Node>, StorageError> {
        match tree {
            Node::Stored { hash, data } => {
                self.store_chunk(*hash, data)?;
                Ok(Arc::new(Node::Skipped {
                    hash: *hash,
                    size: data.len() as u64,
                }))
            }
            Node::Parent {
                hash, left, right, ..
            } => {
                let left = self.try_fill_in_compact(left)?;
                let right = self.try_fill_in_compact(right)?;
                self.store_compact_link(*hash, left, right)
            }
            Node::Skipped { hash, size } => {
                if self.contains_hash(hash) {
                    Ok(Arc::new(Node::Skipped {
                        hash: *hash,
                        size: *size,
                    }))
                } else {
                    Err(StorageError::TreeReconstruct)
                }
            }
        }
    }

    fn handle_for_path(&self, path: &Path) -> Option<Arc<Mutex<Handle>>> {
        self.handles.read().unwrap().get(path).cloned()
    }

    fn is_path_fully_populated(&self, path: &Path) -> bool {
        self.inner
            .read()
            .unwrap()
            .data
            .values()
            .flat_map(|entries| entries.iter())
            .filter(|entry| entry.path == path)
            .all(|entry| entry.populated.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn flush_if_path_complete(&self, path: &Path) -> Result<(), Error> {
        if self.is_path_fully_populated(path) {
            if let Some(handle) = self.handle_for_path(path) {
                handle.lock().unwrap().flush()?;
            }
        }
        Ok(())
    }

    fn ensure_handle(&self, path: &Path) -> Result<(), Error> {
        if self.handle_for_path(path).is_some() {
            return Ok(());
        }

        let mut handles = self.handles.write().unwrap();
        if !handles.contains_key(path) {
            handles.insert(path.to_owned(), Arc::new(Mutex::new(Handle::new(path)?)));
        }
        Ok(())
    }

    fn flush_handles(&self) -> Result<(), Error> {
        let handles = self
            .handles
            .read()
            .unwrap()
            .iter()
            .map(|(path, handle)| (path.clone(), handle.clone()))
            .collect::<Vec<_>>();
        for (path, handle) in handles {
            handle.lock().unwrap().flush().inspect_err(|e| {
                tracing::error!("Cannot flush handle {}: {e}", path.to_string_lossy())
            })?;
        }
        Ok(())
    }

    pub fn receive_sync_item(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        manifest: &Manifest,
        chunk_infos: &[ChunkInfo],
        local_hashes: &[Hash],
        received_chunks: Vec<Vec<u8>>,
    ) -> Result<Item, Error> {
        self.receive_sync_item_shared(
            name,
            path,
            revision,
            description,
            manifest,
            chunk_infos,
            local_hashes,
            received_chunks
                .into_iter()
                .map(ReceivedChunk::from_vec)
                .collect(),
        )
    }

    pub fn receive_sync_item_shared(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        manifest: &Manifest,
        chunk_infos: &[ChunkInfo],
        local_hashes: &[Hash],
        received_chunks: Vec<ReceivedChunk>,
    ) -> Result<Item, Error> {
        if chunk_infos.len() != manifest.chunk_count as usize {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }

        let stored_path = self.path(&path)?;
        let available_hashes: HashSet<Hash> = local_hashes.iter().copied().collect();
        let mut received_chunks: VecDeque<ReceivedChunk> = received_chunks.into();
        let mut staged_data = HashMap::default();
        let mut received_in_order = Vec::with_capacity(received_chunks.len());
        let (chunks, root) = if manifest.entries.is_empty() {
            let mut partials = Vec::with_capacity(chunk_infos.len());
            let mut chunks = Vec::with_capacity(chunk_infos.len());

            for chunk_info in chunk_infos.iter().copied() {
                chunks.push(chunk_info);

                let node = if available_hashes.contains(&chunk_info.hash) {
                    let data = self
                        .read_chunk_data(&chunk_info.hash)
                        .ok_or(Error::Storage(StorageError::TreeReconstruct))?;
                    staged_data.insert(chunk_info.hash, data);
                    Arc::new(Node::Skipped {
                        hash: chunk_info.hash,
                        size: chunk_info.size,
                    })
                } else {
                    let data = received_chunks
                        .pop_front()
                        .ok_or_else(|| Error::Storage(StorageError::TreeReconstruct))?;
                    if do_hash(&data) != chunk_info.hash {
                        return Err(Error::Storage(StorageError::TreeReconstruct));
                    }
                    received_in_order.push((chunk_info.hash, data.clone()));
                    staged_data.insert(chunk_info.hash, ChunkData::Shared(data));
                    Arc::new(Node::Skipped {
                        hash: chunk_info.hash,
                        size: chunk_info.size,
                    })
                };

                partials.push(node);
            }

            let root = self.finalize_sync_partials(partials)?;
            (chunks, root)
        } else {
            self.receive_manifest_entry_chunks(
                manifest,
                chunk_infos,
                &available_hashes,
                &mut received_chunks,
                &mut staged_data,
                &mut received_in_order,
            )?
        };

        if !received_chunks.is_empty() {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }

        if root.hash() != &manifest.root_hash || root.size() != manifest.total_size {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }

        self.activate_staged_chunks(&stored_path, manifest, &chunks, &staged_data)?;

        self.cache_verified_received_chunks(received_in_order);

        let item = Item::make_with_entries(
            name,
            path,
            revision,
            description,
            root.chunk_info(),
            chunks,
            manifest.entries.clone(),
            manifest.chunk_algorithm,
        )?;

        let mut inner = self.inner.write().unwrap();
        inner
            .items
            .retain(|existing| existing.metadata.artifact_id != item.metadata.artifact_id);
        inner.insert_item(item.clone());
        inner.retain_links_reachable_from_items();
        drop(inner);
        self.persist()?;
        Ok(item)
    }

    fn persist(&self) -> Result<(), Error> {
        let _single_writer = self.persistence_lock.lock().unwrap();
        self.flush_handles()?;
        let (canonical, generation, persistence_path) = {
            let inner = self.inner.read().unwrap();
            (
                FsStorageStateV1::from_runtime(&inner)?,
                inner.generation,
                inner.persistance_path.clone(),
            )
        };
        let payload = bitcode::serialize(&canonical)
            .inspect_err(|error| tracing::error!("Serialization error: {error}"))
            .map_err(InvalidParameter::from)?;
        replace_enveloped_state(
            &persistence_path,
            DurableStateKind::FsStorage,
            FS_STORAGE_PAYLOAD_SCHEMA,
            &payload,
            MAX_DURABLE_STATE_PAYLOAD,
        )?;
        let mut inner = self.inner.write().unwrap();
        if inner.generation == generation {
            inner.dirty = false;
        }
        Ok(())
    }

    /// Create a new `FsStorage` with a root path.
    ///
    /// If persistence data exists it will be reloaded; otherwise an empty storage is created.
    /// Any unreadable, corrupt, or semantically invalid state fails closed.
    pub fn new(root: PathBuf) -> Result<Self, Error> {
        Self::with_cache_config(root, FsStorageCacheConfig::default())
    }

    pub fn with_cache_config(
        requested_root: PathBuf,
        cache_config: FsStorageCacheConfig,
    ) -> Result<Self, Error> {
        let persistance_dir = cache_dir().join("chunk_storage").join("fs_storage");
        let legacy_path =
            persistance_dir.join(requested_root.to_string_lossy().replace('/', "___"));
        create_dir_all(&persistance_dir)?;
        create_dir_all(&requested_root).inspect(|()| {
            tracing::info!("Created root path '{}'", requested_root.to_string_lossy());
        })?;
        let root = if requested_root.is_absolute() {
            requested_root.clone()
        } else {
            std::env::current_dir()?.join(&requested_root)
        };
        let root_identity = fs_storage_root_identity(&root)?;
        let persistance_path = persistance_dir.join(identity_filename(&root_identity));

        let state = match fs::symlink_metadata(&persistance_path) {
            Ok(_) => {
                tracing::debug!("FsStorage envelope found, loading…");
                let payload = read_enveloped_state(
                    &persistance_path,
                    DurableStateKind::FsStorage,
                    FS_STORAGE_PAYLOAD_SCHEMA,
                    MAX_DURABLE_STATE_PAYLOAD,
                )?;
                let canonical: FsStorageStateV1 =
                    bitcode::deserialize(&payload).map_err(InvalidParameter::from)?;
                canonical.into_runtime(root.clone(), persistance_path.clone())?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::symlink_metadata(&legacy_path) {
                    Ok(_) => {
                        tracing::info!("Legacy FsStorage state found, migrating…");
                        let bytes = read_bounded_file(&legacy_path, MAX_DURABLE_STATE_PAYLOAD)?;
                        let legacy: LegacyFsStorageState =
                            bitcode::deserialize(&bytes).map_err(InvalidParameter::from)?;
                        let expected = legacy.into_v1(&root, &legacy_path)?;
                        let payload =
                            bitcode::serialize(&expected).map_err(InvalidParameter::from)?;
                        replace_enveloped_state(
                            &persistance_path,
                            DurableStateKind::FsStorage,
                            FS_STORAGE_PAYLOAD_SCHEMA,
                            &payload,
                            MAX_DURABLE_STATE_PAYLOAD,
                        )?;

                        let reread_payload = read_enveloped_state(
                            &persistance_path,
                            DurableStateKind::FsStorage,
                            FS_STORAGE_PAYLOAD_SCHEMA,
                            MAX_DURABLE_STATE_PAYLOAD,
                        )?;
                        let actual: FsStorageStateV1 = bitcode::deserialize(&reread_payload)
                            .map_err(InvalidParameter::from)?;
                        if actual != expected {
                            return Err(invalid_state(
                                "migrated state did not reread with semantic equivalence",
                            ));
                        }
                        actual.into_runtime(root.clone(), persistance_path.clone())?
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        tracing::debug!("No previous FsStorage state found, creating a new one");
                        FsStorageState {
                            root,
                            persistance_path,
                            ..Default::default()
                        }
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        };

        // Opening writable handles is deliberately last: bounded decode and all
        // constructor-root/path semantics are validated before this point.
        let mut handles = HashMap::default();
        for entries in state.data.values() {
            for entry in entries {
                if !handles.contains_key(&entry.path) {
                    handles.insert(
                        entry.path.clone(),
                        Arc::new(Mutex::new(Handle::existing(&entry.path)?)),
                    );
                }
            }
        }

        let storage = Self {
            inner: RwLock::new(state),
            persistence_lock: Mutex::new(()),
            handles: RwLock::new(handles),
            chunk_cache: Mutex::new(HotChunkCache::new(cache_config.max_chunk_bytes)),
            tree_cache: Mutex::new(HotNodeCache::new(cache_config.max_tree_entries)),
        };
        if storage.migrate_referenced_chunks_to_cas()? {
            storage.persist()?;
        }
        Ok(storage)
    }

    /// Returns the stored path of any path relative to root
    #[must_use]
    pub fn path(&self, path: &Path) -> Result<PathBuf, Error> {
        self.inner.read().unwrap().path(path)
    }

    /// Full on-disk path for an item
    pub fn item_path(&self, item: &Item) -> Result<PathBuf, Error> {
        self.inner.read().unwrap().item_path(item)
    }

    /// Return the most recent item tracked for the given local path.
    #[must_use]
    pub fn item_for_path(&self, path: &Path) -> Option<Item> {
        let inner = self.inner.read().unwrap();
        let full_path = inner.path(path).ok()?;
        inner
            .items
            .iter()
            .filter(|item| {
                inner
                    .path(&item.metadata.path)
                    .is_ok_and(|item_path| item_path == full_path)
            })
            .cloned()
            .max_by_key(|item| (item.metadata.revision, item.chunks.len(), item.size()))
    }

    /// Pre-allocate a single `ChunkInfo` slot on disk
    pub fn pre_allocate_chunk(
        &self,
        path: &Path,
        chunk_info: &ChunkInfo,
        offset: u64,
    ) -> Result<(), Error> {
        self.inner
            .write()
            .unwrap()
            .pre_allocate_chunk(path, chunk_info, offset)?;
        self.ensure_handle(path)
    }

    /// Pre-allocate space for multiple `ChunkInfo` slots on disk
    pub fn pre_allocate(&self, path: &Path, data: &[ChunkInfo]) -> Result<(), Error> {
        self.inner.write().unwrap().pre_allocate(path, data)?;
        self.ensure_handle(path)
    }

    /// Pre-allocate space for raw bytes on disk
    pub fn pre_allocate_bytes(&self, path: &Path, data: &[u8]) -> Result<(), Error> {
        self.inner.write().unwrap().pre_allocate_bytes(path, data)?;
        self.ensure_handle(path)
    }

    /// Pre-allocate space for an item in the filesystem
    pub fn pre_allocate_item(&self, item: &Item) -> Result<(), Error> {
        let mut inner = self.inner.write().unwrap();
        if inner.items.contains(item) {
            return Ok(());
        }
        let path = inner.item_path(item)?;
        tracing::debug!("Preallocating item to {:?}", path);
        inner.pre_allocate(&path, &item.chunks[..])?;
        inner.insert_item(item.clone());
        drop(inner);
        self.ensure_handle(&path)?;
        self.persist()
    }

    /// Remove references to a file from storage (does not delete the file from disk)
    pub fn remove(&self, item: Item) -> Result<(), Error> {
        let mut inner = self.inner.write().unwrap();
        let path = inner.item_path(&item)?;
        let item = inner
            .remove_item(&item)
            .then_some(item)
            .ok_or(Error::MissingData)?;
        for chunk in &item.chunks {
            let remove_key = if let Some(infile_chunks) = inner.data.get_mut(&chunk.hash) {
                infile_chunks.retain(|infile_chunk| infile_chunk.path != path);
                infile_chunks.is_empty()
            } else {
                false
            };

            if remove_key {
                inner.data.remove(&chunk.hash);
            }
        }
        drop(inner);
        self.handles.write().unwrap().remove(&path);
        self.persist()
    }

    /// Remove references to a file from storage and delete the file from disk
    pub fn delete(&self, item: Item) -> Result<(), Error> {
        let path = self.item_path(&item)?;
        self.remove(item)
            .and_then(|()| remove_file(path).map_err(Error::IoError))
    }
}

impl ChunkStorage for FsStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<Node>> {
        let linked = self.inner.read().unwrap().links.get(hash).cloned();
        if linked.is_some() {
            return self.materialize_node(linked?);
        }

        self.stored_node(hash)
    }

    fn size(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner
            .data
            .keys()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .filter_map(|hash| inner.data.get(&hash).and_then(|entries| entries.first()))
            .filter(|entry| entry.populated.load(std::sync::atomic::Ordering::Relaxed))
            .map(|entry| entry.info.size)
            .sum()
    }

    fn chunk_list(&self, root: &Hash) -> Vec<Hash> {
        self.item_for_root_hash(root)
            .map(|item| item.chunks.iter().map(|chunk| chunk.hash).collect())
            .unwrap_or_else(|| {
                self.get(root)
                    .and_then(|node| node.flatten().ok())
                    .unwrap_or_default()
            })
    }

    fn get_chunk_by_index(&self, root: &Hash, index: u32) -> Option<Vec<u8>> {
        if let Some(item) = self.item_for_root_hash(root) {
            let chunk = item.chunks.get(index as usize)?;
            return self
                .read_chunk_data(&chunk.hash)
                .map(|data| data.as_ref().to_vec());
        }

        let node = self.get(root)?;
        let leaves = node.flatten_iter().ok()?;
        leaves.get(index as usize).map(|arc| (**arc).clone())
    }

    fn store_chunk(&self, hash: Hash, chunk: &[u8]) -> Result<Arc<Node>, StorageError> {
        if do_hash(chunk) != hash {
            return Err(StorageError::ImmutableConflict);
        }
        // `InFileChunk::write` flips its populated atomic. Serialize that
        // transition with persistence so no envelope can publish populated
        // metadata before the corresponding bytes have been flushed.
        let _persistence = self.persistence_lock.lock().unwrap();
        self.flush_handles()
            .map_err(|_| StorageError::ChunkInsertError)?;
        let (root, infile_chunks) = {
            let inner = self.inner.read().unwrap();
            (
                inner.root.clone(),
                inner
                    .data
                    .get(&hash)
                    .cloned()
                    .ok_or(StorageError::ChunkInsertError)?,
            )
        };

        // Validate every target before the first write. A single altered or
        // inconsistent backing must not permit partial mutation of siblings.
        for infile_chunk in &infile_chunks {
            if infile_chunk.info.hash != hash || infile_chunk.info.size != chunk.len() as u64 {
                tracing::error!(
                    "immutable chunk metadata mismatch: {:?} supplied {}",
                    infile_chunk.info,
                    chunk.len()
                );
                return Err(StorageError::ImmutableConflict);
            }
            if infile_chunk
                .populated
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                let relative = relative_state_path(&root, &infile_chunk.path)
                    .inspect_err(|error| {
                        tracing::error!("immutable backing relative path: {error}")
                    })
                    .map_err(|_| StorageError::ImmutableConflict)?;
                let path = validate_existing_chunk_path(
                    &root,
                    &relative,
                    true,
                    infile_chunk.offset,
                    infile_chunk.info.size,
                )
                .inspect_err(|error| tracing::error!("immutable backing validation: {error}"))
                .map_err(|_| StorageError::ImmutableConflict)?;
                let actual = hash_file_range(&path, infile_chunk.offset, infile_chunk.info.size)
                    .inspect_err(|error| tracing::error!("immutable backing read: {error}"))
                    .map_err(|_| StorageError::ImmutableConflict)?;
                if actual != hash {
                    tracing::error!(
                        "immutable backing hash mismatch: expected {hash}, got {actual} at {}+{}",
                        path.display(),
                        infile_chunk.offset
                    );
                    return Err(StorageError::ImmutableConflict);
                }
            }
        }

        for infile_chunk in &infile_chunks {
            tracing::trace!("infile chunk {infile_chunk:?}");
            if let Some(handle) = self.handle_for_path(&infile_chunk.path) {
                infile_chunk
                    .write(&hash, chunk, &mut handle.lock().unwrap())
                    .inspect(|()| {
                        tracing::trace!(
                            "Written infile chunk {hash} to {}",
                            infile_chunk.path.to_string_lossy()
                        )
                    })
                    .inspect_err(|e| {
                        tracing::error!(
                            "Cannot write infile chunk to {}: {e}",
                            infile_chunk.path.to_string_lossy()
                        )
                    })
                    .map_err(|_| StorageError::ChunkInsertError)?;
            } else {
                let mut handle =
                    Handle::new(&infile_chunk.path).map_err(|_| StorageError::ChunkInsertError)?;
                infile_chunk
                    .write(&hash, chunk, &mut handle)
                    .inspect(|()| {
                        tracing::trace!(
                            "Written infile chunk {hash} to {} with transient handle",
                            infile_chunk.path.to_string_lossy()
                        )
                    })
                    .inspect_err(|e| {
                        tracing::error!(
                            "Cannot write infile chunk to {}: {e}",
                            infile_chunk.path.to_string_lossy()
                        )
                    })
                    .map_err(|_| StorageError::ChunkInsertError)?;
                handle.flush().map_err(|_| StorageError::ChunkInsertError)?;
            }
        }

        for path in infile_chunks
            .iter()
            .map(|entry| entry.path.clone())
            .collect::<HashSet<_>>()
        {
            self.flush_if_path_complete(&path)
                .map_err(|_| StorageError::ChunkInsertError)?;
        }

        self.inner.write().unwrap().mark_dirty();
        let data = self.cache_chunk_data(hash, Arc::new(chunk.to_vec()));
        Ok(Arc::new(Node::Stored { hash, data }))
    }

    fn store_link(
        &self,
        hash: Hash,
        left: Arc<Node>,
        right: Arc<Node>,
    ) -> Result<Arc<Node>, StorageError> {
        self.inner.write().unwrap().store_link(hash, left, right)
    }

    fn create_item(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        file: bytes::Bytes,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        tracing::debug!("Create item {name} with path {path:?}");
        let mut inner = self.inner.write().unwrap();
        let stored_path = inner.path(&path)?;
        create_dir_all(stored_path.parent().ok_or(Error::MissingData)?)?;
        let chunks: Vec<ChunkInfo> = chunk_algorithm
            .chunk_boundaries(file.as_ref())
            .into_iter()
            .map(|(offset, length)| {
                let chunk = &file[offset..offset + length];
                ChunkInfo {
                    hash: do_hash(chunk),
                    size: length as u64,
                }
            })
            .collect();
        inner.pre_allocate(&stored_path, &chunks)?;
        drop(inner);
        self.ensure_handle(&stored_path)?;
        if let Some(handle) = self.handle_for_path(&stored_path) {
            handle
                .lock()
                .unwrap()
                .set_len(chunks.iter().map(|chunk| chunk.size).sum())?;
        }
        let (hash_tree, chunks) = self.build_compact_tree(file.as_ref(), chunk_algorithm)?;
        let item = Item::make(
            name,
            path,
            revision,
            description,
            hash_tree.chunk_info(),
            chunks,
            chunk_algorithm,
        )?;
        self.inner.write().unwrap().insert_item(item.clone());
        self.persist()?;
        Ok(item)
    }

    fn create_item_from_files(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        mut files: Vec<(PathBuf, bytes::Bytes)>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        if files.is_empty() {
            return Err(Error::MissingData);
        }

        files.sort_by(|left, right| left.0.cmp(&right.0));

        let item_root = self.path(&path)?;
        create_dir_all(&item_root)?;
        let (hash_tree, chunks, entries) =
            self.build_compact_tree_from_files(&item_root, &files, chunk_algorithm)?;
        for entry in &entries {
            let path = self.path(&Self::artifact_entry_path(
                &item_root,
                Path::new(&entry.relative_path),
            )?)?;
            if let Some(handle) = self.handle_for_path(&path) {
                handle.lock().unwrap().set_len(entry.size)?;
            }
        }
        let item = Item::make_with_entries(
            name,
            path,
            revision,
            description,
            hash_tree.chunk_info(),
            chunks,
            entries,
            chunk_algorithm,
        )?;
        self.inner.write().unwrap().insert_item(item.clone());
        self.persist()?;
        Ok(item)
    }

    fn commit_revision(&self, item: Item, file: bytes::Bytes) -> Result<Item, Error>
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
            return Err(invalid_state(
                "prepared revision does not exactly match supplied bytes",
            ));
        }
        let boundaries = if file.is_empty() {
            vec![(0, 0)]
        } else {
            item.manifest
                .chunk_algorithm
                .chunk_boundaries(file.as_ref())
        };
        if boundaries.len() != item.chunks.len() {
            return Err(invalid_state(
                "supplied revision has a different chunk count than prepared metadata",
            ));
        }
        for ((offset, length), chunk) in boundaries.into_iter().zip(item.chunks.iter().copied()) {
            self.commit_cas_chunk(chunk, &file[offset..offset + length])?;
        }
        self.finish_cas_revision(item)
    }

    fn commit_revision_from_files(
        &self,
        item: Item,
        mut files: Vec<(PathBuf, bytes::Bytes)>,
    ) -> Result<Item, Error>
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
            return Err(invalid_state(
                "prepared directory revision does not exactly match supplied files",
            ));
        }
        if files.is_empty() {
            return Err(Error::MissingData);
        }
        files.sort_by(|left, right| left.0.cmp(&right.0));
        if files.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(invalid_state("directory revision contains duplicate paths"));
        }
        let supplied_entry_count = files.len();
        let mut next_chunk = 0usize;
        for (entry_index, (path, data)) in files.into_iter().enumerate() {
            let path = crate::utils::path::validate_relative(&path)?;
            let path = path
                .to_str()
                .ok_or_else(|| invalid_state("directory revision paths must be valid UTF-8"))?;
            let start = u32::try_from(next_chunk).map_err(InvalidParameter::from)?;
            let boundaries = if data.is_empty() {
                vec![(0, 0)]
            } else {
                item.manifest
                    .chunk_algorithm
                    .chunk_boundaries(data.as_ref())
            };
            for (offset, length) in boundaries {
                let chunk = item.chunks.get(next_chunk).copied().ok_or_else(|| {
                    invalid_state("supplied directory has more chunks than prepared metadata")
                })?;
                self.commit_cas_chunk(chunk, &data[offset..offset + length])?;
                next_chunk += 1;
            }
            let end = u32::try_from(next_chunk).map_err(InvalidParameter::from)?;
            let expected_entry = item.manifest.entries.get(entry_index).ok_or_else(|| {
                invalid_state("supplied directory has more entries than prepared metadata")
            })?;
            if expected_entry.relative_path != path
                || expected_entry.size != data.len() as u64
                || expected_entry.chunk_range != (start, end)
            {
                return Err(invalid_state(
                    "supplied directory entry does not match prepared metadata",
                ));
            }
        }
        if item.manifest.entries.len() != supplied_entry_count {
            return Err(invalid_state(
                "supplied directory entry count does not match prepared metadata",
            ));
        }
        if next_chunk != item.chunks.len() {
            return Err(invalid_state(
                "supplied directory has fewer chunks than prepared metadata",
            ));
        }
        self.finish_cas_revision(item)
    }

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
        tracing::debug!("Create item {name} with path {path:?}");
        let mut inner = self.inner.write().unwrap();
        let stored_path = inner.path(&path)?;
        inner.pre_allocate(&stored_path, &root.flatten_with_sizes()?)?;
        drop(inner);
        self.ensure_handle(&stored_path)?;
        tracing::info!("Preallocated on disk {:?}", stored_path);
        let mut inner = self.inner.write().unwrap();
        let item = Item::new(name, path, revision, description, &root, chunk_algorithm);
        tracing::debug!("New item: {item}");
        inner.insert_item(item.clone());
        drop(inner);
        self.persist()?;
        Ok(item)
    }

    /// Build a new Item from its metadata and a streaming of nodes
    async fn receive_item<T>(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        mut stream: T,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, crate::error::Error>
    where
        Self: Sized,
        T: Stream<Item = Node> + std::marker::Unpin,
    {
        let stored_path = self.path(&path)?;
        self.ensure_handle(&stored_path)?;
        tracing::trace!("Receiving item at '{}'", stored_path.to_string_lossy());
        let mut i = 0;
        let mut o = 0u64;
        let mut chunks = Vec::new();
        let mut hashes = HashSet::default();
        let mut last: Option<Arc<Node>> = None;

        while let Some(node) = stream.next().await {
            hashes.insert(node.chunk_info());
            match &node {
                s_n @ Node::Stored { .. } => {
                    tracing::trace!(
                        "Preallocating {} bytes in {}@'{}'",
                        s_n.size(),
                        o,
                        stored_path.to_string_lossy()
                    );
                    chunks.push(s_n.chunk_info());
                    self.pre_allocate_chunk(&stored_path, &s_n.chunk_info(), o)?;
                    o += s_n.size();
                }
                skipped @ Node::Skipped { .. } => {
                    tracing::trace!(
                        "Skipping {} bytes already present at {}@'{}'",
                        skipped.size(),
                        o,
                        stored_path.to_string_lossy()
                    );
                    chunks.push(skipped.chunk_info());
                    o += skipped.size();
                }
                Node::Parent { .. } => {}
            }
            last = Some(self.try_fill_in_compact(&node)?);
            i += 1;
        }

        let last = last.ok_or(StorageError::TreeReconstruct)?;
        tracing::info!("Reconstructed {i} nodes with {} bytes total", last.size());
        let item = Item::make(
            name,
            path,
            revision,
            description,
            last.chunk_info(),
            chunks,
            chunk_algorithm,
        )?;
        let mut inner = self.inner.write().unwrap();
        inner.insert_item(item.clone());
        drop(inner);
        self.persist()?;
        Ok(item)
    }

    fn chunks(&self) -> Vec<Hash> {
        self.inner.read().unwrap().data.keys().copied().collect()
    }
}

impl HashTreeCapable<Arc<Node>, crate::error::Error> for FsStorage {
    fn func(&self, data: &[u8]) -> Result<Arc<Node>, crate::error::Error> {
        self.insert_chunk(data).map_err(Into::into)
    }

    fn merge(&self, l: &Arc<Node>, r: &Arc<Node>) -> Result<Arc<Node>, crate::error::Error> {
        self.link(l.clone(), r.clone()).map_err(Into::into)
    }
}

impl Drop for FsStorage {
    fn drop(&mut self) {
        let dirty = self.inner.read().unwrap().dirty;
        if dirty {
            if let Err(e) = self.persist() {
                tracing::error!("Cannot persist FsStorage on drop: {e}");
            }
        } else if let Err(e) = self.flush_handles() {
            tracing::error!("Cannot flush FsStorage handles on drop: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        chunks::{ChunkAlgorithm, CHUNK_SIZE},
        hash::hash as do_hash,
        item::tests::{make_ones_item, new_dummy_item},
        utils::testing::temp_path,
    };
    use std::str::FromStr;

    use super::*;

    fn print_fsstorage(storage: &FsStorage) {
        let inner = storage.inner.read().unwrap();
        println!(
            "root: {:?} \n\
            chunks: {:?} \n\
            file chunks: {:?} \n\
            items: {:?}",
            inner.root,
            inner.data.keys().collect::<Vec<_>>(),
            inner.data.iter().collect::<Vec<_>>(),
            inner.items,
        );
    }

    fn make_infile_chunk<const SIZE: usize>() -> ([u8; SIZE], Hash, InFileChunk) {
        // Create infile_chunk with some data
        let data = [1u8; SIZE];
        let hash = do_hash(&data);
        let infile_chunk = InFileChunk {
            info: ChunkInfo {
                hash,
                size: SIZE as u64,
            },
            path: PathBuf::new(),
            offset: 0,
            populated: Arc::default(),
        };
        (data, hash, infile_chunk)
    }

    #[test]
    fn immutable_revisions_with_same_logical_path_survive_restart() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let algorithm = ChunkAlgorithm::default();
        let first_bytes = bytes::Bytes::from(vec![1u8; CHUNK_SIZE * 2]);
        let mut second_vec = first_bytes.to_vec();
        second_vec[CHUNK_SIZE] = 2;
        let second_bytes = bytes::Bytes::from(second_vec);

        let first = storage
            .prepare_revision(
                "stable".to_string(),
                "display".to_string(),
                PathBuf::from("same.bin"),
                0,
                None,
                first_bytes.clone(),
                algorithm,
            )
            .unwrap();
        let first = storage.commit_revision(first, first_bytes.clone()).unwrap();
        let second = storage
            .prepare_revision(
                "stable".to_string(),
                "display".to_string(),
                PathBuf::from("same.bin"),
                1,
                None,
                second_bytes.clone(),
                algorithm,
            )
            .unwrap();
        let second = storage
            .commit_revision(second, second_bytes.clone())
            .unwrap();

        assert_eq!(
            storage
                .get(&first.metadata.root.hash)
                .unwrap()
                .clone_data()
                .unwrap(),
            first_bytes.to_vec()
        );
        assert_eq!(
            storage
                .get(&second.metadata.root.hash)
                .unwrap()
                .clone_data()
                .unwrap(),
            second_bytes.to_vec()
        );
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        assert_eq!(
            restarted
                .get(&first.metadata.root.hash)
                .unwrap()
                .clone_data()
                .unwrap(),
            first_bytes.to_vec()
        );
        assert_eq!(
            restarted
                .get(&second.metadata.root.hash)
                .unwrap()
                .clone_data()
                .unwrap(),
            second_bytes.to_vec()
        );
    }

    #[test]
    fn immutable_directory_revision_keeps_removed_and_renamed_entries() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let algorithm = ChunkAlgorithm::default();
        let first_files = vec![
            (PathBuf::from("a.txt"), bytes::Bytes::from_static(b"alpha")),
            (PathBuf::from("gone.txt"), bytes::Bytes::from_static(b"old")),
        ];
        let second_files = vec![
            (
                PathBuf::from("renamed.txt"),
                bytes::Bytes::from_static(b"alpha"),
            ),
            (PathBuf::from("new.txt"), bytes::Bytes::from_static(b"new")),
        ];
        let first = storage
            .prepare_revision_from_files(
                "directory".to_string(),
                "directory".to_string(),
                PathBuf::from("same-dir"),
                0,
                None,
                first_files.clone(),
                algorithm,
            )
            .unwrap();
        let first = storage
            .commit_revision_from_files(first, first_files)
            .unwrap();
        let second = storage
            .prepare_revision_from_files(
                "directory".to_string(),
                "directory".to_string(),
                PathBuf::from("same-dir"),
                1,
                None,
                second_files.clone(),
                algorithm,
            )
            .unwrap();
        storage
            .commit_revision_from_files(second, second_files)
            .unwrap();
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        assert_eq!(
            restarted.get_chunk_by_index(&first.metadata.root.hash, 0),
            Some(b"alpha".to_vec())
        );
        assert_eq!(
            restarted.get_chunk_by_index(&first.metadata.root.hash, 1),
            Some(b"old".to_vec())
        );
    }

    #[cfg(unix)]
    #[test]
    fn immutable_chunk_cas_rejects_symbolic_link_conflict() {
        use std::os::unix::fs::symlink;

        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let bytes = bytes::Bytes::from_static(b"cas-conflict");
        let item = storage
            .prepare_revision(
                "stable".to_string(),
                "stable".to_string(),
                PathBuf::from("logical"),
                0,
                None,
                bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        let cas_path = storage.revision_chunk_path(&item.chunks[0].hash).unwrap();
        fs::create_dir_all(cas_path.parent().unwrap()).unwrap();
        let outside = temp_path();
        fs::write(&outside, bytes.as_ref()).unwrap();
        symlink(&outside, &cas_path).unwrap();

        assert!(storage.commit_revision(item, bytes).is_err());
    }

    #[test]
    fn immutable_revision_delta_adds_only_unique_chunk_bytes() {
        fn file_bytes(path: &Path) -> u64 {
            fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap())
                .map(|entry| {
                    let metadata = entry.metadata().unwrap();
                    if metadata.is_dir() {
                        file_bytes(&entry.path())
                    } else {
                        metadata.len()
                    }
                })
                .sum()
        }

        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let algorithm = ChunkAlgorithm::default();
        let first_bytes = bytes::Bytes::from(vec![1u8; 16 * 1024 * 1024]);
        let first = storage
            .prepare_revision(
                "stable".to_string(),
                "stable".to_string(),
                PathBuf::from("logical"),
                0,
                None,
                first_bytes.clone(),
                algorithm,
            )
            .unwrap();
        storage.commit_revision(first, first_bytes.clone()).unwrap();
        let before = file_bytes(&root.join(FS_STORAGE_REVISION_NAMESPACE));

        let mut changed = first_bytes.to_vec();
        changed[CHUNK_SIZE] = 2;
        let changed = bytes::Bytes::from(changed);
        let second = storage
            .prepare_revision(
                "stable".to_string(),
                "stable".to_string(),
                PathBuf::from("logical"),
                1,
                None,
                changed.clone(),
                algorithm,
            )
            .unwrap();
        storage.commit_revision(second, changed).unwrap();
        let added = file_bytes(&root.join(FS_STORAGE_REVISION_NAMESPACE)) - before;

        assert!(added <= (CHUNK_SIZE as u64 * 11) / 10);
    }

    #[test]
    fn forged_revisions_are_rejected_before_cas_or_index_mutation() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let bytes = bytes::Bytes::from_static(b"forged-revision");
        let mut forged = storage
            .prepare_revision(
                "stable".to_string(),
                "display".to_string(),
                PathBuf::from("logical.bin"),
                0,
                None,
                bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        let root_hash = forged.metadata.root.hash;
        let cas_path = storage.revision_chunk_path(&forged.chunks[0].hash).unwrap();
        forged.manifest.artifact_id = "forged-authority".to_string();

        assert!(storage.commit_revision(forged, bytes).is_err());
        assert!(!cas_path.exists());
        assert!(storage.get(&root_hash).is_none());

        let files = vec![(
            PathBuf::from("entry.txt"),
            bytes::Bytes::from_static(b"directory"),
        )];
        let mut forged_directory = storage
            .prepare_revision_from_files(
                "stable-directory".to_string(),
                "display".to_string(),
                PathBuf::from("logical-dir"),
                0,
                None,
                files.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        let directory_root = forged_directory.metadata.root.hash;
        let directory_cas = storage
            .revision_chunk_path(&forged_directory.chunks[0].hash)
            .unwrap();
        forged_directory.manifest.entries[0].size += 1;

        assert!(storage
            .commit_revision_from_files(forged_directory, files)
            .is_err());
        assert!(!directory_cas.exists());
        assert!(storage.get(&directory_root).is_none());
    }

    #[test]
    fn generic_storage_rejects_forged_chunk_and_link_keys_without_mutation() {
        let storage = FsStorage::new(temp_path()).unwrap();
        let bytes = b"chunk";
        let forged_hash = do_hash(b"different");
        assert!(matches!(
            storage.store_chunk(forged_hash, bytes),
            Err(StorageError::ImmutableConflict)
        ));
        assert!(!storage
            .inner
            .read()
            .unwrap()
            .data
            .contains_key(&forged_hash));

        let left = Arc::new(Node::Skipped {
            hash: do_hash(b"left"),
            size: 4,
        });
        let right = Arc::new(Node::Skipped {
            hash: do_hash(b"right"),
            size: 5,
        });
        let correct = merge_hashes(left.hash(), right.hash());
        assert!(matches!(
            storage.store_link(do_hash(b"wrong"), left.clone(), right.clone()),
            Err(StorageError::ImmutableConflict)
        ));
        assert!(storage.inner.read().unwrap().links.is_empty());

        storage
            .store_link(correct, left.clone(), right.clone())
            .unwrap();
        let conflicting_left = Arc::new(Node::Skipped {
            hash: *left.hash(),
            size: left.size() + 1,
        });
        assert!(matches!(
            storage.store_link(correct, conflicting_left, right),
            Err(StorageError::ImmutableConflict)
        ));
        assert_eq!(storage.inner.read().unwrap().links.len(), 1);
    }

    #[test]
    fn store_chunk_rejects_altered_populated_backing_before_sibling_mutation() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let supplied = b"verified";
        let info = ChunkInfo {
            hash: do_hash(supplied),
            size: supplied.len() as u64,
        };
        let altered_path = root.join("altered.bin");
        let untouched_path = root.join("untouched.bin");
        storage.pre_allocate_chunk(&altered_path, &info, 0).unwrap();
        storage
            .pre_allocate_chunk(&untouched_path, &info, 0)
            .unwrap();
        fs::write(&altered_path, b"tampered").unwrap();
        let untouched = vec![0x5a; supplied.len()];
        fs::write(&untouched_path, &untouched).unwrap();
        {
            let inner = storage.inner.read().unwrap();
            inner.data[&info.hash]
                .iter()
                .find(|entry| entry.path == altered_path)
                .unwrap()
                .populated
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        assert!(matches!(
            storage.store_chunk(info.hash, supplied),
            Err(StorageError::ImmutableConflict)
        ));
        assert_eq!(fs::read(&altered_path).unwrap(), b"tampered");
        assert_eq!(fs::read(&untouched_path).unwrap(), untouched);
        let inner = storage.inner.read().unwrap();
        assert!(!inner.data[&info.hash]
            .iter()
            .find(|entry| entry.path == untouched_path)
            .unwrap()
            .populated
            .load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn store_chunk_rejects_unpopulated_size_mismatch_without_write() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let supplied = b"short";
        let info = ChunkInfo {
            hash: do_hash(supplied),
            size: supplied.len() as u64 + 1,
        };
        let path = root.join("sized.bin");
        storage.pre_allocate_chunk(&path, &info, 0).unwrap();
        let original = vec![0x33; supplied.len() + 1];
        fs::write(&path, &original).unwrap();

        assert!(matches!(
            storage.store_chunk(info.hash, supplied),
            Err(StorageError::ImmutableConflict)
        ));
        assert_eq!(fs::read(&path).unwrap(), original);
        assert!(!storage.inner.read().unwrap().data[&info.hash][0]
            .populated
            .load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn duplicate_current_revision_identity_fails_without_rewriting_source() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        for bytes in [
            bytes::Bytes::from_static(b"first"),
            bytes::Bytes::from_static(b"second"),
        ] {
            let item = storage
                .prepare_revision(
                    "same-artifact".to_string(),
                    "display".to_string(),
                    PathBuf::from("logical"),
                    0,
                    None,
                    bytes.clone(),
                    ChunkAlgorithm::default(),
                )
                .unwrap();
            storage.commit_revision(item, bytes).unwrap();
        }
        let current_path = storage.inner.read().unwrap().persistance_path.clone();
        let source = fs::read(&current_path).unwrap();
        drop(storage);

        assert!(FsStorage::new(root).is_err());
        assert_eq!(fs::read(current_path).unwrap(), source);
    }

    #[test]
    fn normalized_alias_in_current_directory_fails_without_rewriting_source() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let files = vec![(
            PathBuf::from("nested/file.txt"),
            bytes::Bytes::from_static(b"entry"),
        )];
        let item = storage
            .prepare_revision_from_files(
                "directory".to_string(),
                "directory".to_string(),
                PathBuf::from("logical"),
                0,
                None,
                files.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        storage.commit_revision_from_files(item, files).unwrap();
        let current_path = storage.inner.read().unwrap().persistance_path.clone();
        let payload = read_enveloped_state(
            &current_path,
            DurableStateKind::FsStorage,
            FS_STORAGE_PAYLOAD_SCHEMA,
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap();
        let mut state: FsStorageStateV1 = bitcode::deserialize(&payload).unwrap();
        state.items[0].manifest.entries[0].relative_path = "nested/./file.txt".to_string();
        let payload = bitcode::serialize(&state).unwrap();
        replace_enveloped_state(
            &current_path,
            DurableStateKind::FsStorage,
            FS_STORAGE_PAYLOAD_SCHEMA,
            &payload,
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap();
        let source = fs::read(&current_path).unwrap();
        drop(storage);

        assert!(FsStorage::new(root).is_err());
        assert_eq!(fs::read(current_path).unwrap(), source);
    }

    #[test]
    fn semantically_invalid_current_item_fails_without_rewriting_source() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let bytes = bytes::Bytes::from_static(b"current-state");
        let item = storage
            .prepare_revision(
                "stable".to_string(),
                "stable".to_string(),
                PathBuf::from("logical"),
                0,
                None,
                bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        storage.commit_revision(item, bytes).unwrap();
        let current_path = storage.inner.read().unwrap().persistance_path.clone();
        let payload = read_enveloped_state(
            &current_path,
            DurableStateKind::FsStorage,
            FS_STORAGE_PAYLOAD_SCHEMA,
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap();
        let mut state: FsStorageStateV1 = bitcode::deserialize(&payload).unwrap();
        state.items[0].manifest.chunk_count += 1;
        let payload = bitcode::serialize(&state).unwrap();
        replace_enveloped_state(
            &current_path,
            DurableStateKind::FsStorage,
            FS_STORAGE_PAYLOAD_SCHEMA,
            &payload,
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap();
        let invalid_source = fs::read(&current_path).unwrap();
        drop(storage);

        assert!(FsStorage::new(root).is_err());
        assert_eq!(fs::read(current_path).unwrap(), invalid_source);
    }

    #[test]
    fn semantically_invalid_legacy_item_fails_without_rewriting_source() {
        let requested_root = temp_path();
        fs::create_dir_all(&requested_root).unwrap();
        let root = fs::canonicalize(&requested_root).unwrap();
        let bytes = b"legacy-invalid";
        let info = ChunkInfo {
            hash: do_hash(bytes),
            size: bytes.len() as u64,
        };
        let artifact_path = root.join("artifact.bin");
        fs::write(&artifact_path, bytes).unwrap();
        let mut item = Item::make(
            "legacy".to_string(),
            PathBuf::from("artifact.bin"),
            0,
            None,
            info,
            vec![info],
            ChunkAlgorithm::default(),
        )
        .unwrap();
        item.manifest.version = 1;
        let persistance_dir = cache_dir().join("chunk_storage").join("fs_storage");
        fs::create_dir_all(&persistance_dir).unwrap();
        let legacy_path =
            persistance_dir.join(requested_root.to_string_lossy().replace('/', "___"));
        let current_path =
            persistance_dir.join(identity_filename(&fs_storage_root_identity(&root).unwrap()));
        let legacy = LegacyFsStorageState {
            root,
            items: HashSet::from_iter([item]),
            persistance_path: legacy_path.clone(),
            data: HashMap::from_iter([(
                info.hash,
                vec![Arc::new(InFileChunk {
                    info,
                    path: artifact_path,
                    offset: 0,
                    populated: Arc::new(AtomicBool::new(true)),
                })],
            )]),
            links: HashMap::default(),
        };
        let source = bitcode::serialize(&legacy).unwrap();
        fs::write(&legacy_path, &source).unwrap();

        assert!(FsStorage::new(requested_root).is_err());
        assert_eq!(fs::read(legacy_path).unwrap(), source);
        assert!(!current_path.exists());
    }

    #[test]
    fn duplicate_legacy_revision_identity_fails_without_rewriting_source() {
        let requested_root = temp_path();
        fs::create_dir_all(&requested_root).unwrap();
        let root = fs::canonicalize(&requested_root).unwrap();
        let mut items = HashSet::default();
        let mut data = HashMap::default();
        for (index, bytes) in [b"legacy-one".as_slice(), b"legacy-two".as_slice()]
            .into_iter()
            .enumerate()
        {
            let info = ChunkInfo {
                hash: do_hash(bytes),
                size: bytes.len() as u64,
            };
            let relative = PathBuf::from(format!("artifact-{index}.bin"));
            let path = root.join(&relative);
            fs::write(&path, bytes).unwrap();
            let item = Item::make(
                "same-artifact".to_string(),
                relative,
                0,
                None,
                info,
                vec![info],
                ChunkAlgorithm::default(),
            )
            .unwrap();
            items.insert(item);
            data.insert(
                info.hash,
                vec![Arc::new(InFileChunk {
                    info,
                    path,
                    offset: 0,
                    populated: Arc::new(AtomicBool::new(true)),
                })],
            );
        }
        let persistance_dir = cache_dir().join("chunk_storage").join("fs_storage");
        fs::create_dir_all(&persistance_dir).unwrap();
        let legacy_path =
            persistance_dir.join(requested_root.to_string_lossy().replace('/', "___"));
        let current_path =
            persistance_dir.join(identity_filename(&fs_storage_root_identity(&root).unwrap()));
        let source = bitcode::serialize(&LegacyFsStorageState {
            root,
            items,
            persistance_path: legacy_path.clone(),
            data,
            links: HashMap::default(),
        })
        .unwrap();
        fs::write(&legacy_path, &source).unwrap();

        assert!(FsStorage::new(requested_root).is_err());
        assert_eq!(fs::read(legacy_path).unwrap(), source);
        assert!(!current_path.exists());
    }

    #[test]
    fn normalized_alias_in_legacy_directory_fails_without_rewriting_source() {
        let requested_root = temp_path();
        fs::create_dir_all(&requested_root).unwrap();
        let root = fs::canonicalize(&requested_root).unwrap();
        let bytes = b"legacy-directory";
        let info = ChunkInfo {
            hash: do_hash(bytes),
            size: bytes.len() as u64,
        };
        let artifact_path = root.join("artifact.bin");
        fs::write(&artifact_path, bytes).unwrap();
        let item = Item::make_with_entries(
            "directory".to_string(),
            PathBuf::from("logical"),
            0,
            None,
            info,
            vec![info],
            vec![FileEntry {
                relative_path: "nested/./file.txt".to_string(),
                size: info.size,
                chunk_range: (0, 1),
            }],
            ChunkAlgorithm::default(),
        )
        .unwrap();
        let persistance_dir = cache_dir().join("chunk_storage").join("fs_storage");
        fs::create_dir_all(&persistance_dir).unwrap();
        let legacy_path =
            persistance_dir.join(requested_root.to_string_lossy().replace('/', "___"));
        let current_path =
            persistance_dir.join(identity_filename(&fs_storage_root_identity(&root).unwrap()));
        let source = bitcode::serialize(&LegacyFsStorageState {
            root,
            items: HashSet::from_iter([item]),
            persistance_path: legacy_path.clone(),
            data: HashMap::from_iter([(
                info.hash,
                vec![Arc::new(InFileChunk {
                    info,
                    path: artifact_path,
                    offset: 0,
                    populated: Arc::new(AtomicBool::new(true)),
                })],
            )]),
            links: HashMap::default(),
        })
        .unwrap();
        fs::write(&legacy_path, &source).unwrap();

        assert!(FsStorage::new(requested_root).is_err());
        assert_eq!(fs::read(legacy_path).unwrap(), source);
        assert!(!current_path.exists());
    }

    #[test]
    fn hot_chunk_cache_accounts_shared_backings_once_across_views_and_replacements() {
        let backing = ReceivedChunk::from_vec(Vec::from(&b"abcdefghijkl"[..]));
        let first = backing.slice(0..4);
        let second = backing.slice(4..8);
        let replacement = backing.slice(8..12);
        let first_hash = do_hash(first.as_ref());
        let second_hash = do_hash(second.as_ref());
        let mut cache = HotChunkCache::new(12);

        cache.insert_shared(first_hash, first);
        cache.insert_shared(second_hash, second);
        assert_eq!(cache.total_bytes, 12);
        assert_eq!(cache.shared_backings.len(), 1);
        assert_eq!(
            cache.shared_backings.values().next().unwrap().cached_views,
            2
        );

        cache.insert_shared(first_hash, replacement);
        assert_eq!(cache.total_bytes, 12);
        assert_eq!(
            cache.shared_backings.values().next().unwrap().cached_views,
            2
        );
    }

    #[test]
    fn hot_chunk_cache_evicts_every_view_before_unaccounting_shared_backing() {
        let backing = ReceivedChunk::from_vec(Vec::from(&b"abcdefghijkl"[..]));
        let first = backing.slice(0..4);
        let second = backing.slice(4..8);
        let other = ReceivedChunk::from_vec(Vec::from(&b"12345678"[..]));
        let first_hash = do_hash(first.as_ref());
        let second_hash = do_hash(second.as_ref());
        let other_hash = do_hash(other.as_ref());
        let other_backing_id = other.backing_id();
        let mut cache = HotChunkCache::new(12);

        cache.insert_shared(first_hash, first);
        cache.insert_shared(second_hash, second);
        cache.insert_shared(other_hash, other);

        assert_eq!(cache.total_bytes, 8);
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.entries.contains_key(&other_hash));
        assert_eq!(cache.shared_backings.len(), 1);
        assert!(cache.shared_backings.contains_key(&other_backing_id));
    }

    #[test]
    fn hot_chunk_cache_repeated_hash_view_keeps_one_full_backing_charge() {
        let backing = ReceivedChunk::from_vec(Vec::from(&b"samesame"[..]));
        let first = backing.slice(0..4);
        let repeated = backing.slice(4..8);
        let hash = do_hash(first.as_ref());
        assert_eq!(hash, do_hash(repeated.as_ref()));
        let mut cache = HotChunkCache::new(8);

        cache.insert_shared(hash, first);
        cache.insert_shared(hash, repeated);

        assert_eq!(cache.total_bytes, 8);
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(
            cache.shared_backings.values().next().unwrap().cached_views,
            1
        );
    }

    #[test]
    fn verified_received_cache_replays_manifest_order_with_repeated_hash_before_eviction() {
        let storage = FsStorage::with_cache_config(
            temp_path(),
            FsStorageCacheConfig {
                max_chunk_bytes: 8,
                max_tree_entries: 1,
            },
        )
        .unwrap();
        let first_a = ReceivedChunk::from_vec(b"aaaa".to_vec());
        let b = ReceivedChunk::from_vec(b"bbbb".to_vec());
        let refreshed_a = ReceivedChunk::from_vec(b"aaaa".to_vec());
        let c = ReceivedChunk::from_vec(b"cccc".to_vec());
        let a_hash = do_hash(first_a.as_ref());
        let b_hash = do_hash(b.as_ref());
        let c_hash = do_hash(c.as_ref());
        let refreshed_a_backing = refreshed_a.backing_id();
        let c_backing = c.backing_id();

        storage.cache_verified_received_chunks(vec![
            (a_hash, first_a),
            (b_hash, b),
            (a_hash, refreshed_a),
            (c_hash, c),
        ]);

        let cache = storage.chunk_cache.lock().unwrap();
        assert_eq!(
            cache.order,
            VecDeque::from([a_hash, c_hash]),
            "repeated A must refresh after B, so later pressure evicts B"
        );
        assert_eq!(
            cache.entries.keys().copied().collect::<HashSet<_>>(),
            HashSet::from_iter([a_hash, c_hash])
        );
        assert_eq!(cache.total_bytes, 8);
        assert_eq!(
            cache
                .shared_backings
                .keys()
                .copied()
                .collect::<HashSet<_>>(),
            HashSet::from_iter([refreshed_a_backing, c_backing])
        );
    }

    fn write_data_to_infile_chunk(
        data: &[u8],
        hash: Hash,
        infile_chunk: &mut InFileChunk,
    ) -> PathBuf {
        //  Add a temporary path
        let path = std::env::temp_dir().join(PathBuf::from_str("tempfile").unwrap());
        let mut write_buf = Handle::new(&path).unwrap();

        infile_chunk.path = path.clone();
        infile_chunk.offset = 0;

        // write to temp path
        infile_chunk.write(&hash, data, &mut write_buf).unwrap();
        write_buf.flush().unwrap();

        path
    }

    fn is_populated(infile_chunk: &InFileChunk) -> bool {
        infile_chunk
            .populated
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[test]
    fn infile_chunk() {
        let (data, hash, mut infile_chunk) = make_infile_chunk::<CHUNK_SIZE>();
        println!("Created infile_chunk");
        assert!(!is_populated(&infile_chunk));

        let path = write_data_to_infile_chunk(&data, hash, &mut infile_chunk);
        assert!(is_populated(&infile_chunk));

        // Check written data
        let mut f = File::open(&path).unwrap();
        #[allow(clippy::large_stack_arrays)]
        let mut buffer = [0u8; CHUNK_SIZE];
        let n = f.read(&mut buffer[..]).unwrap();
        assert_eq!(n, CHUNK_SIZE);
        assert_eq!(do_hash(&buffer), hash);
    }

    #[test]
    fn infile_chunk_conversion() {
        let (data, hash, mut infile_chunk) = make_infile_chunk::<CHUNK_SIZE>();
        let _path = write_data_to_infile_chunk(&data, hash, &mut infile_chunk);

        assert!(is_populated(&infile_chunk));

        let chunk = Node::try_from(infile_chunk.clone()).unwrap();
        assert_eq!(do_hash(&chunk.stored_data().unwrap()), do_hash(&data));
        assert_eq!(chunk.hash(), &hash);
        assert_eq!(do_hash(&data), hash);

        // Check idempotence
        let chunk2 = Node::try_from(infile_chunk).unwrap();
        assert_eq!(chunk, chunk2);
    }

    #[test]
    fn fs_storage() {
        // check default doesn't panic, just in case
        let storage = FsStorage::default();
        print_fsstorage(&storage);

        // create storage in a temporary directory
        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone()).unwrap();

        // make an item with a known content, a single chunk of all ones
        let item = make_ones_item().unwrap();
        storage.pre_allocate_item(&item).unwrap();

        // Actually store the item chunk
        #[allow(clippy::large_stack_arrays)]
        storage.insert_chunk(&[1u8; CHUNK_SIZE]).unwrap();
        print_fsstorage(&storage);

        assert!(matches!(
            storage.get(&do_hash(&[1u8; CHUNK_SIZE])),
            Some(..)
        ));

        let itempath = crate::utils::path::join(&tempdir, &item.metadata.path);
        assert!(itempath.exists());

        let path = storage.item_path(&item).unwrap();
        assert_eq!(itempath, path);
        let mut f = File::open(&path).unwrap();
        let mut buffer = vec![0u8; item.size() as usize];

        println!("Stored data path {path:?}");

        // read from file
        let n = f.read(&mut buffer[..]).unwrap();

        assert_eq!(n, usize::try_from(item.size()).unwrap());
        assert_eq!(do_hash(&buffer), item.metadata.root.hash);

        // retrieve chunk from storage
        assert_eq!(
            do_hash(
                storage
                    .get(&item.metadata.root.hash)
                    .unwrap()
                    .stored_data()
                    .unwrap()
                    .as_ref()
            ),
            item.metadata.root.hash
        );
    }

    #[test]
    fn fs_storage_round_trip() {
        // create storage in a temporary directory
        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone()).unwrap();

        let item = new_dummy_item::<FsStorage, 1u8, 1_000_000>(&storage).unwrap();
        println!("Created item: {item:?}");
        print_fsstorage(&storage);

        let stored = storage
            .get(&item.metadata.root.hash)
            .unwrap()
            .clone_data()
            .unwrap();

        // reported storage size is deduplicated
        assert_eq!(stored.len(), 1_000_000);
        for b in stored {
            assert_eq!(b, 1u8);
        }

        // check for data on disk to match the expected one
        let file = std::fs::read(storage.item_path(&item).unwrap()).unwrap();
        assert_eq!(file.len(), 1_000_000);
        for b in file {
            assert_eq!(b, 1u8);
        }
    }

    #[test]
    fn fs_storage_create_item_preserves_logical_path() {
        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone()).unwrap();
        let logical_path = PathBuf::from("bench-artifact");

        let item = storage
            .create_item(
                "bench-artifact".to_string(),
                logical_path.clone(),
                0,
                None,
                bytes::Bytes::from_static(b"hello world"),
                ChunkAlgorithm::default(),
            )
            .unwrap();

        assert_eq!(item.metadata.path, logical_path);
        assert_eq!(
            storage.item_path(&item).unwrap(),
            tempdir.join("bench-artifact")
        );
    }

    #[test]
    fn fs_storage_chunk_lookup_uses_item_metadata() {
        let tempdir = temp_path();
        let storage = FsStorage::with_cache_config(
            tempdir.clone(),
            FsStorageCacheConfig {
                max_chunk_bytes: CHUNK_SIZE * 2,
                max_tree_entries: FsStorageCacheConfig::default().max_tree_entries,
            },
        )
        .unwrap();
        let data: Vec<u8> = (0..(CHUNK_SIZE * 3 + 17))
            .map(|index| ((index * 5) % 251) as u8)
            .collect();

        let item = storage
            .create_item(
                "bench-artifact".to_string(),
                PathBuf::from("bench-artifact"),
                0,
                None,
                bytes::Bytes::from(data.clone()),
                ChunkAlgorithm::default(),
            )
            .unwrap();

        let listed = storage.chunk_list(&item.metadata.root.hash);
        let expected: Vec<_> = item.chunks.iter().map(|chunk| chunk.hash).collect();
        assert_eq!(listed, expected);

        for (index, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
            assert_eq!(
                storage.get_chunk_by_index(&item.metadata.root.hash, index as u32),
                Some(chunk.to_vec())
            );
        }
    }

    #[test]
    fn fs_storage_create_item_supports_fastcdc_chunks() {
        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone()).unwrap();
        let data: Vec<u8> = (0..(CHUNK_SIZE * 3 + CHUNK_SIZE / 2))
            .map(|index| ((index * 17) % 251) as u8)
            .collect();

        let item = storage
            .create_item(
                "fastcdc-item".to_string(),
                PathBuf::from("fastcdc-item"),
                0,
                None,
                bytes::Bytes::from(data.clone()),
                ChunkAlgorithm::fastcdc_default(),
            )
            .unwrap();

        assert_eq!(item.size(), data.len() as u64);
        assert!(!item.chunks.is_empty());
        assert_eq!(
            item.manifest.chunk_algorithm,
            ChunkAlgorithm::fastcdc_default()
        );
        let stored = storage
            .get(&item.metadata.root.hash)
            .unwrap()
            .clone_data()
            .unwrap();
        assert_eq!(stored, data);
    }

    #[tokio::test]
    async fn fs_storage_receive_item_round_trip() {
        use crate::chunk_storage::hashmap_storage::HashMapStorage;

        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone()).unwrap();
        let source = HashMapStorage::default();
        let data = vec![7u8; CHUNK_SIZE * 4 + 123];
        let root = source.insert(data.clone().into()).unwrap();
        let stream = tokio_stream::iter(root.find_diff(&[]).map(|node| (*node).clone()));

        let item = storage
            .receive_item(
                "received-item".to_string(),
                tempdir.join("received.bin"),
                1,
                None,
                stream,
                ChunkAlgorithm::default(),
            )
            .await
            .unwrap();

        let stored = storage
            .get(&item.metadata.root.hash)
            .unwrap()
            .clone_data()
            .unwrap();
        assert_eq!(stored, data);
    }

    #[tokio::test]
    async fn fs_storage_receive_sync_item_sparse_round_trip() {
        use crate::chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage};
        use crate::item::Manifest;

        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone()).unwrap();
        let source = HashMapStorage::default();

        let v1: Vec<u8> = (0..(CHUNK_SIZE * 8))
            .map(|index| ((index * 7) % 251) as u8)
            .collect();
        let old_root = source.insert(v1.clone().into()).unwrap();
        let old_stream =
            tokio_stream::iter(old_root.clone().find_diff(&[]).map(|node| (*node).clone()));
        let old_item = storage
            .receive_item(
                "sync-item".to_string(),
                tempdir.join("sync.bin"),
                1,
                None,
                old_stream,
                ChunkAlgorithm::default(),
            )
            .await
            .unwrap();

        let mut v2 = v1.clone();
        for block_index in [2usize, 6usize] {
            let start = block_index * CHUNK_SIZE;
            let end = start + CHUNK_SIZE;
            for (offset, byte) in v2[start..end].iter_mut().enumerate() {
                *byte = byte.wrapping_add((block_index as u8).wrapping_mul(13)) ^ (offset as u8);
            }
        }

        let new_root = source.insert(v2.clone().into()).unwrap();
        let chunk_infos = new_root.flatten_with_sizes().unwrap();
        let manifest = Manifest::from_tree(
            "sync-item".to_string(),
            2,
            &new_root,
            &chunk_infos,
            ChunkAlgorithm::default(),
        );
        let local_hashes = old_root.flatten().unwrap();
        let received_chunks: Vec<ReceivedChunk> = v2
            .chunks(CHUNK_SIZE)
            .enumerate()
            .filter(|(index, _)| matches!(*index, 2 | 6))
            .map(|(_, chunk)| ReceivedChunk::copy_from_slice(chunk))
            .collect();

        let item = storage
            .receive_sync_item_shared(
                old_item.metadata.name,
                old_item.metadata.path,
                2,
                None,
                &manifest,
                &chunk_infos,
                &local_hashes,
                received_chunks,
            )
            .unwrap();

        let stored = storage
            .get(&item.metadata.root.hash)
            .unwrap()
            .clone_data()
            .unwrap();
        assert_eq!(stored, v2);

        // A later update must be able to use a previously received chunk from
        // the activated destination even though it was not copied into the hot
        // chunk cache.
        let mut v3 = v2.clone();
        let changed_start = 6 * CHUNK_SIZE;
        for byte in &mut v3[changed_start..changed_start + CHUNK_SIZE] {
            *byte = byte.wrapping_add(41);
        }
        let v3_root = source.insert(v3.clone().into()).unwrap();
        let v3_chunk_infos = v3_root.flatten_with_sizes().unwrap();
        let v3_manifest = Manifest::from_tree(
            "sync-item".to_string(),
            3,
            &v3_root,
            &v3_chunk_infos,
            ChunkAlgorithm::default(),
        );
        let v2_hashes = new_root.flatten().unwrap();
        let v3_received = vec![ReceivedChunk::copy_from_slice(
            &v3[changed_start..changed_start + CHUNK_SIZE],
        )];

        let v3_item = storage
            .receive_sync_item_shared(
                item.metadata.name,
                item.metadata.path,
                3,
                None,
                &v3_manifest,
                &v3_chunk_infos,
                &v2_hashes,
                v3_received,
            )
            .unwrap();
        let stored_v3 = storage
            .get(&v3_item.metadata.root.hash)
            .unwrap()
            .clone_data()
            .unwrap();
        assert_eq!(stored_v3, v3);
    }

    #[tokio::test]
    async fn fs_storage_legacy_receive_sync_item_vec_round_trip() {
        use crate::chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage};
        use crate::item::Manifest;

        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone()).unwrap();
        let source = HashMapStorage::default();

        let data: Vec<u8> = (0..(CHUNK_SIZE * 5 + 123))
            .map(|index| ((index * 11) % 251) as u8)
            .collect();
        let root = source.insert(data.clone().into()).unwrap();
        let chunk_infos = root.flatten_with_sizes().unwrap();
        let manifest = Manifest::from_tree(
            "odd-item".to_string(),
            1,
            &root,
            &chunk_infos,
            ChunkAlgorithm::default(),
        );
        let received_chunks: Vec<Vec<u8>> = data
            .chunks(CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();

        let item = storage
            .receive_sync_item(
                "odd-item".to_string(),
                tempdir.join("odd.bin"),
                1,
                None,
                &manifest,
                &chunk_infos,
                &[],
                received_chunks,
            )
            .unwrap();

        let stored = storage
            .get(&item.metadata.root.hash)
            .unwrap()
            .clone_data()
            .unwrap();
        assert_eq!(stored, data);
    }

    #[test]
    fn fs_storage_shared_receive_keeps_exact_bulk_slices_through_activation_and_cache() {
        use crate::chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage};
        use crate::item::Manifest;

        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let source = HashMapStorage::default();
        let mut data = vec![1u8; CHUNK_SIZE];
        data.extend(vec![2u8; CHUNK_SIZE]);
        let tree = source.chunk_tree(&data, ChunkAlgorithm::Fixed256K).unwrap();
        let manifest = Manifest::from_tree(
            "shared-item".to_string(),
            1,
            &tree.0,
            &tree.1,
            ChunkAlgorithm::Fixed256K,
        );
        let backing = ReceivedChunk::from_vec(data.clone());
        let backing_id = backing.backing_id();
        let chunks = vec![
            backing.slice(0..CHUNK_SIZE),
            backing.slice(CHUNK_SIZE..data.len()),
        ];

        storage
            .receive_sync_item_shared(
                "shared-item".to_string(),
                PathBuf::from("shared.bin"),
                1,
                None,
                &manifest,
                &tree.1,
                &[],
                chunks,
            )
            .unwrap();

        let cache = storage.chunk_cache.lock().unwrap();
        assert_eq!(cache.total_bytes, data.len());
        for chunk in &tree.1 {
            let ChunkData::Shared(cached) = cache.entries.get(&chunk.hash).unwrap() else {
                panic!("received chunk was copied into owned cache storage");
            };
            assert_eq!(cached.backing_id(), backing_id);
        }
        drop(cache);
        assert_eq!(std::fs::read(root.join("shared.bin")).unwrap(), data);
    }

    #[test]
    fn fs_storage_persistance() {
        // create storage in a temporary directory
        let tempdir = temp_path();

        let item: Option<Item>;
        let item_hash: Option<Hash>;

        // create storage and let it go out of scope
        {
            let storage = FsStorage::new(tempdir.clone()).unwrap();

            // save item and hash
            item = Some(new_dummy_item::<FsStorage, 1u8, 1_000_000>(&storage).unwrap());
            item_hash = Some(item.as_ref().unwrap().metadata.root.hash);
            println!("Created item: {item:?}");
            println!("Item hash: {}", item_hash.unwrap());

            print_fsstorage(&storage);
        }

        // Then re-create storage and retrieve the data
        println!("Reloading storage");
        let storage = FsStorage::new(tempdir.clone()).unwrap();
        print_fsstorage(&storage);

        // Check for contained item
        {
            let inner = storage.inner.read().unwrap();
            assert_eq!(inner.items.len(), 1);
            let retrieved = inner.items.iter().next().unwrap();
            let item = item.unwrap();
            assert_eq!(retrieved.metadata, item.metadata);
            assert_eq!(retrieved.metadata, item.metadata);
        }

        // Check data: same as fs_storage_roundtrip
        {
            println!("{:?}", storage.chunks());
            let stored = storage
                .get(&item_hash.unwrap())
                .unwrap()
                .clone_data()
                .unwrap();

            // reported storage size is deduplicated
            assert_eq!(stored.len(), 1_000_000);
            for b in stored {
                assert_eq!(b, 1u8);
            }
        }
    }

    #[test]
    fn fs_storage_persistance_10x() {
        // repeated test to check determinism
        for _ in 0..10 {
            fs_storage_persistance();
        }
    }

    #[test]
    fn canonical_state_encoding_ignores_hash_insertion_order() {
        let requested_root = temp_path();
        fs::create_dir_all(&requested_root).unwrap();
        let root = fs::canonicalize(&requested_root).unwrap();
        let first_path = root.join("first.bin");
        let second_path = root.join("second.bin");
        fs::write(&first_path, b"first").unwrap();
        fs::write(&second_path, b"second").unwrap();
        let first_info = ChunkInfo {
            hash: do_hash(b"first"),
            size: 5,
        };
        let second_info = ChunkInfo {
            hash: do_hash(b"second"),
            size: 6,
        };

        let make_state = |reverse: bool| {
            let mut data = HashMap::default();
            let mut entries = vec![
                (first_info, first_path.clone()),
                (second_info, second_path.clone()),
            ];
            if reverse {
                entries.reverse();
            }
            for (info, path) in entries {
                data.insert(
                    info.hash,
                    vec![Arc::new(InFileChunk {
                        info,
                        path,
                        offset: 0,
                        populated: Arc::new(AtomicBool::new(true)),
                    })],
                );
            }
            FsStorageState {
                root: root.clone(),
                persistance_path: PathBuf::from("ignored"),
                data,
                ..Default::default()
            }
        };

        let forward =
            bitcode::serialize(&FsStorageStateV1::from_runtime(&make_state(false)).unwrap())
                .unwrap();
        let reverse =
            bitcode::serialize(&FsStorageStateV1::from_runtime(&make_state(true)).unwrap())
                .unwrap();
        assert_eq!(forward, reverse);
    }

    #[test]
    fn current_payload_golden_fixture_is_stable() {
        let payload = bitcode::serialize(&FsStorageStateV1 {
            root_identity: [0x11; 32],
            items: Vec::new(),
            data: Vec::new(),
            links: Vec::new(),
        })
        .unwrap();
        let hex = payload
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            hex,
            "1111111111111111111111111111111111111111111111111111111111111111000000"
        );
    }

    #[test]
    fn flat_link_writer_accepts_exact_record_limit_and_rejects_one_more() {
        let mut records = Vec::with_capacity(FS_STORAGE_MAX_LINK_RECORDS);
        for index in 0..FS_STORAGE_MAX_LINK_RECORDS {
            let pushed = push_flat_link_record(
                &mut records,
                FsStorageNodeV1::Skipped {
                    hash: Hash::from_bytes([index as u8; 32]),
                    size: 1,
                },
            )
            .unwrap();
            assert_eq!(pushed as usize, index);
        }
        assert_eq!(records.len(), FS_STORAGE_MAX_LINK_RECORDS);
        assert!(push_flat_link_record(
            &mut records,
            FsStorageNodeV1::Skipped {
                hash: Hash::from_bytes([0xff; 32]),
                size: 1,
            },
        )
        .is_err());
        assert_eq!(records.len(), FS_STORAGE_MAX_LINK_RECORDS);
    }

    #[test]
    fn over_depth_flat_link_is_rejected_before_materialization() {
        let mut records = vec![
            FsStorageNodeV1::Skipped {
                hash: do_hash(b"root-left"),
                size: 1,
            },
            FsStorageNodeV1::Skipped {
                hash: do_hash(b"root-right"),
                size: 1,
            },
        ];
        let mut previous = 0_u32;
        for depth in 1..=FS_STORAGE_MAX_LINK_DEPTH + 1 {
            let leaf = u32::try_from(records.len()).unwrap();
            let leaf_hash = do_hash(&depth.to_le_bytes());
            records.push(FsStorageNodeV1::Skipped {
                hash: leaf_hash,
                size: 1,
            });
            let (left_hash, left_size) = match &records[previous as usize] {
                FsStorageNodeV1::Skipped { hash, size }
                | FsStorageNodeV1::Parent { hash, size, .. } => (*hash, *size),
            };
            let hash = merge_hashes(&left_hash, &leaf_hash);
            let parent = u32::try_from(records.len()).unwrap();
            records.push(FsStorageNodeV1::Parent {
                hash,
                size: left_size + 1,
                left: previous,
                right: leaf,
            });
            previous = parent;
        }

        let error = materialize_flat_link(records).unwrap_err();
        assert!(error.to_string().contains("maximum depth"));
    }

    #[test]
    fn frozen_nonempty_legacy_fixture_is_compatible() {
        // Produced once from source a5d7c997 with the Cargo.lock-pinned
        // Bitcode 0.6.3. Keeping literal bytes prevents reader and fixture
        // writer from drifting together.
        const HEX: &str = concat!(
            "0c2f6c65676163792f726f6f74010b6c65676163792d6974656d0b6c65676163792d6974656d010e66726f7a656e206669787475726504070c61727469666163742e62696e060bf97187ad3fe0c34440687ef66bf7ac80ca91407d72ff7966f4c6d6f1c6e0f29b022bcc7d6a00a8e18932022bcc7d6a00a8e1893205302e322e30000b6c65676163792d6974656d0607f97187ad3fe0c34440687ef66bf7ac80ca91407d72ff7966f4c6d6f1c6e0f29b060b04010000000400000001060bf97187ad3fe0c34440687ef66bf7ac80ca91407d72ff7966f4c6d6f1c6e0f29b0d2f6c65676163792f737461746501f97187ad3fe0c34440687ef66bf7ac80ca91407d72ff7966f4c6d6f1c6e0f29b01060bf97187ad3fe0c34440687ef66bf7ac80ca91407d72ff7966f4c6d6f1c6e0f29b192f6c65676163792f726f6f742f61727469666163742e62696e06000101f97187ad3fe0c34440687ef66bf7ac80ca91407d72ff7966f4c6d6f1c6e0f29b02f97187ad3fe0c34440687ef66bf7ac80ca91407d72ff7966f4c6d6f1c6e0f29b060b"
        );
        let bytes = (0..HEX.len())
            .step_by(2)
            .map(|offset| u8::from_str_radix(&HEX[offset..offset + 2], 16).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            blake3::hash(&bytes).to_hex().as_str(),
            "d4b23e5e1c4ec21a40e44a15530277bf97c5ecdcf73bb0b72edc57510cbeab9d"
        );
        let legacy: LegacyFsStorageState = bitcode::deserialize(&bytes).unwrap();
        assert_eq!(legacy.root, Path::new("/legacy/root"));
        assert_eq!(legacy.persistance_path, Path::new("/legacy/state"));
        assert_eq!(legacy.items.len(), 1);
        assert_eq!(legacy.data.len(), 1);
        assert_eq!(legacy.links.len(), 1);

        // The frozen bytes intentionally contain absolute historical paths and
        // no external payload bytes. Relocate path and byte authority into a
        // portable, self-consistent representation before exercising the real
        // constructor migration; the literal decode assertions above remain
        // the compatibility check for the historical DTO.
        let requested_root = temp_path();
        fs::create_dir_all(&requested_root).unwrap();
        let root = fs::canonicalize(&requested_root).unwrap();
        let artifact_path = root.join("artifact.bin");
        let fixture_bytes = b"legacy-data";
        fs::write(&artifact_path, fixture_bytes).unwrap();
        let persistance_dir = cache_dir().join("chunk_storage").join("fs_storage");
        fs::create_dir_all(&persistance_dir).unwrap();
        let legacy_path =
            persistance_dir.join(requested_root.to_string_lossy().replace('/', "___"));
        let current_path =
            persistance_dir.join(identity_filename(&fs_storage_root_identity(&root).unwrap()));

        let mut relocatable = legacy;
        relocatable.root = root.clone();
        relocatable.persistance_path = legacy_path.clone();
        let relocated_info = ChunkInfo {
            hash: do_hash(fixture_bytes),
            size: fixture_bytes.len() as u64,
        };
        let mut relocated_items = HashSet::default();
        for mut item in relocatable.items.drain() {
            item.metadata.root = relocated_info;
            item.manifest.root_hash = relocated_info.hash;
            item.manifest.total_size = relocated_info.size;
            item.chunks = vec![relocated_info];
            relocated_items.insert(item);
        }
        relocatable.items = relocated_items;
        let mut relocated_entries = relocatable.data.drain().next().unwrap().1;
        for entry in &mut relocated_entries {
            let entry = Arc::make_mut(entry);
            entry.info = relocated_info;
            entry.path = artifact_path.clone();
            entry.offset = 0;
        }
        relocatable
            .data
            .insert(relocated_info.hash, relocated_entries);
        relocatable.links.clear();
        relocatable.links.insert(
            relocated_info.hash,
            Arc::new(Node::Skipped {
                hash: relocated_info.hash,
                size: relocated_info.size,
            }),
        );
        let expected = relocatable.clone().into_v1(&root, &legacy_path).unwrap();
        assert_eq!(expected.items.len(), 1);
        assert_eq!(expected.data.len(), 1);
        assert_eq!(expected.links.len(), 1);
        assert_eq!(expected.items[0].metadata.path, Path::new("artifact.bin"));
        assert_eq!(
            expected.data[0].chunks[0].relative_path,
            Path::new("artifact.bin")
        );
        assert!(expected.data[0].chunks[0].populated);
        let relocated_legacy_bytes = bitcode::serialize(&relocatable).unwrap();
        fs::write(&legacy_path, &relocated_legacy_bytes).unwrap();
        let storage = FsStorage::new(requested_root.clone()).unwrap();
        assert_eq!(fs::read(&legacy_path).unwrap(), relocated_legacy_bytes);

        let current_payload = read_enveloped_state(
            &current_path,
            DurableStateKind::FsStorage,
            FS_STORAGE_PAYLOAD_SCHEMA,
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap();
        let current: FsStorageStateV1 = bitcode::deserialize(&current_payload).unwrap();
        let runtime_projection = {
            let inner = storage.inner.read().unwrap();
            assert_eq!(inner.items.len(), 1);
            assert_eq!(inner.data.len(), 1);
            FsStorageStateV1::from_runtime(&inner).unwrap()
        };
        assert_eq!(current, runtime_projection);
        assert_eq!(current.items, expected.items);
        assert!(current.data[0].chunks.iter().any(|chunk| {
            chunk
                .relative_path
                .starts_with(FS_STORAGE_REVISION_NAMESPACE)
                && chunk.populated
        }));
        drop(storage);

        let restarted = FsStorage::new(requested_root).unwrap();
        let restart_projection =
            FsStorageStateV1::from_runtime(&restarted.inner.read().unwrap()).unwrap();
        assert_eq!(restart_projection, current);
        assert_eq!(fs::read(legacy_path).unwrap(), relocated_legacy_bytes);
    }

    #[test]
    #[ignore = "local APFS performance reference; run explicitly in release mode"]
    fn durable_state_reference_performance() {
        use std::time::Instant;

        let requested_root = temp_path();
        fs::create_dir_all(&requested_root).unwrap();
        let root = fs::canonicalize(&requested_root).unwrap();
        let mut state = FsStorageState {
            root: root.clone(),
            persistance_path: root.join("state"),
            ..Default::default()
        };
        for item_index in 0..1_000_u32 {
            let relative = PathBuf::from(format!("item-{item_index:04}.bin"));
            let path = root.join(&relative);
            fs::write(&path, [0_u8; 4]).unwrap();
            let mut chunks = Vec::new();
            for chunk_index in 0..4_u32 {
                let identity = [item_index.to_le_bytes(), chunk_index.to_le_bytes()].concat();
                let info = ChunkInfo {
                    hash: do_hash(&identity),
                    size: 1,
                };
                chunks.push(info);
                state.data.insert(
                    info.hash,
                    vec![Arc::new(InFileChunk {
                        info,
                        path: path.clone(),
                        offset: u64::from(chunk_index),
                        populated: Arc::new(AtomicBool::new(true)),
                    })],
                );
            }
            state.items.insert(
                Item::make(
                    format!("item-{item_index:04}"),
                    relative,
                    item_index,
                    None,
                    ChunkInfo {
                        hash: do_hash(&item_index.to_le_bytes()),
                        size: 4,
                    },
                    chunks,
                    ChunkAlgorithm::default(),
                )
                .unwrap(),
            );
        }

        let started = Instant::now();
        let canonical = FsStorageStateV1::from_runtime(&state).unwrap();
        let payload = bitcode::serialize(&canonical).unwrap();
        let framed = crate::durable_state::encode_envelope(
            DurableStateKind::FsStorage,
            FS_STORAGE_PAYLOAD_SCHEMA,
            &payload,
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap();
        let encode = started.elapsed();

        let started = Instant::now();
        let decoded = crate::durable_state::decode_envelope(
            &framed,
            DurableStateKind::FsStorage,
            FS_STORAGE_PAYLOAD_SCHEMA,
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap();
        let decoded: FsStorageStateV1 = bitcode::deserialize(decoded).unwrap();
        let _runtime = decoded
            .into_runtime(root.clone(), root.join("decoded-state"))
            .unwrap();
        let decode = started.elapsed();

        let mut commits = Vec::new();
        for _ in 0..20 {
            let started = Instant::now();
            replace_enveloped_state(
                &root.join("state"),
                DurableStateKind::FsStorage,
                FS_STORAGE_PAYLOAD_SCHEMA,
                &payload,
                MAX_DURABLE_STATE_PAYLOAD,
            )
            .unwrap();
            commits.push(started.elapsed());
        }
        commits.sort_unstable();
        let p95 = commits[18];
        eprintln!(
            "payload={} encode_ms={:.3} decode_ms={:.3} commit_p95_ms={:.3}",
            payload.len(),
            encode.as_secs_f64() * 1_000.0,
            decode.as_secs_f64() * 1_000.0,
            p95.as_secs_f64() * 1_000.0
        );
        assert!(encode.as_secs_f64() < 0.050);
        assert!(decode.as_secs_f64() < 0.120);
        assert!(p95.as_secs_f64() < 0.015);
    }

    #[test]
    fn migrates_only_legacy_path_and_retains_legacy_bytes() {
        let requested_root = temp_path();
        fs::create_dir_all(&requested_root).unwrap();
        let root = fs::canonicalize(&requested_root).unwrap();
        let persistance_dir = cache_dir().join("chunk_storage").join("fs_storage");
        fs::create_dir_all(&persistance_dir).unwrap();
        let legacy_path =
            persistance_dir.join(requested_root.to_string_lossy().replace('/', "___"));
        let current_path =
            persistance_dir.join(identity_filename(&fs_storage_root_identity(&root).unwrap()));
        assert_ne!(legacy_path, current_path);

        let legacy = LegacyFsStorageState {
            root: root.clone(),
            items: HashSet::default(),
            persistance_path: legacy_path.clone(),
            data: HashMap::default(),
            links: HashMap::default(),
        };
        let legacy_bytes = bitcode::serialize(&legacy).unwrap();
        fs::write(&legacy_path, &legacy_bytes).unwrap();

        let storage = FsStorage::new(requested_root.clone()).unwrap();
        assert_eq!(storage.inner.read().unwrap().persistance_path, current_path);
        assert_eq!(fs::read(&legacy_path).unwrap(), legacy_bytes);
        assert!(current_path.exists());
        drop(storage);

        let reloaded = FsStorage::new(requested_root).unwrap();
        assert!(reloaded.inner.read().unwrap().items.is_empty());
        assert_eq!(fs::read(&legacy_path).unwrap(), legacy_bytes);
    }

    #[test]
    fn corrupt_current_state_never_falls_back_to_legacy() {
        let requested_root = temp_path();
        fs::create_dir_all(&requested_root).unwrap();
        let root = fs::canonicalize(&requested_root).unwrap();
        let persistance_dir = cache_dir().join("chunk_storage").join("fs_storage");
        fs::create_dir_all(&persistance_dir).unwrap();
        let legacy_path =
            persistance_dir.join(requested_root.to_string_lossy().replace('/', "___"));
        let current_path =
            persistance_dir.join(identity_filename(&fs_storage_root_identity(&root).unwrap()));
        let legacy = LegacyFsStorageState {
            root,
            items: HashSet::default(),
            persistance_path: legacy_path.clone(),
            data: HashMap::default(),
            links: HashMap::default(),
        };
        fs::write(&legacy_path, bitcode::serialize(&legacy).unwrap()).unwrap();
        let corrupt = b"not an envelope";
        fs::write(&current_path, corrupt).unwrap();

        assert!(FsStorage::new(requested_root).is_err());
        assert_eq!(fs::read(current_path).unwrap(), corrupt);
    }

    #[test]
    fn rejects_destinations_outside_the_storage_root() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let escaped_name = format!("distd-escaped-{}", uuid::Uuid::new_v4());
        let escaped = root.parent().unwrap().join(&escaped_name);

        let result = storage.create_item(
            "unsafe-item".to_string(),
            PathBuf::from("..").join(&escaped_name),
            0,
            None,
            bytes::Bytes::from_static(b"must stay contained"),
            ChunkAlgorithm::default(),
        );

        assert!(result.is_err());
        assert!(!escaped.exists());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symbolic_link_destinations() {
        use std::os::unix::fs::symlink;

        let root = temp_path();
        let outside = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        symlink(&outside, root.join("link")).unwrap();

        assert!(storage.path(Path::new("link/file")).is_err());
        assert!(!outside.join("file").exists());
    }

    #[test]
    fn sync_activation_truncates_a_shorter_revision() {
        use crate::chunk_storage::hashmap_storage::HashMapStorage;

        let root = temp_path();
        let storage = FsStorage::with_cache_config(
            root.clone(),
            FsStorageCacheConfig {
                max_chunk_bytes: 1,
                max_tree_entries: 16,
            },
        )
        .unwrap();
        storage
            .create_item(
                "artifact".to_string(),
                PathBuf::from("artifact.bin"),
                0,
                None,
                bytes::Bytes::from(vec![1u8; CHUNK_SIZE * 3]),
                ChunkAlgorithm::default(),
            )
            .unwrap();

        let replacement = vec![9u8; CHUNK_SIZE / 2];
        let source = HashMapStorage::default();
        let (replacement_root, chunks) = source
            .chunk_tree(&replacement, ChunkAlgorithm::default())
            .unwrap();
        let manifest = Manifest::from_tree(
            "artifact".to_string(),
            1,
            &replacement_root,
            &chunks,
            ChunkAlgorithm::default(),
        );

        storage
            .receive_sync_item(
                "artifact".to_string(),
                PathBuf::from("artifact.bin"),
                1,
                None,
                &manifest,
                &chunks,
                &[],
                vec![replacement.clone()],
            )
            .unwrap();

        assert_eq!(
            std::fs::read(root.join("artifact.bin")).unwrap(),
            replacement
        );
    }

    #[test]
    fn sync_rejects_bad_received_hash_before_destination_mutation() {
        use crate::chunk_storage::hashmap_storage::HashMapStorage;

        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let original = vec![1u8; CHUNK_SIZE];
        storage
            .create_item(
                "artifact".to_string(),
                PathBuf::from("artifact.bin"),
                0,
                None,
                bytes::Bytes::from(original.clone()),
                ChunkAlgorithm::default(),
            )
            .unwrap();

        let expected = vec![9u8; CHUNK_SIZE];
        let source = HashMapStorage::default();
        let (replacement_root, chunks) = source
            .chunk_tree(&expected, ChunkAlgorithm::default())
            .unwrap();
        let manifest = Manifest::from_tree(
            "artifact".to_string(),
            1,
            &replacement_root,
            &chunks,
            ChunkAlgorithm::default(),
        );
        let corrupt = ReceivedChunk::from(vec![7u8; CHUNK_SIZE]);
        let cached_before = {
            let cache = storage.chunk_cache.lock().unwrap();
            (
                cache.entries.keys().copied().collect::<HashSet<_>>(),
                cache.total_bytes,
            )
        };

        let result = storage.receive_sync_item_shared(
            "artifact".to_string(),
            PathBuf::from("artifact.bin"),
            1,
            None,
            &manifest,
            &chunks,
            &[],
            vec![corrupt],
        );

        assert!(result.is_err());
        assert_eq!(std::fs::read(root.join("artifact.bin")).unwrap(), original);
        let cache = storage.chunk_cache.lock().unwrap();
        assert_eq!(
            cache.entries.keys().copied().collect::<HashSet<_>>(),
            cached_before.0,
            "failed received data must not enter the cache"
        );
        assert_eq!(cache.total_bytes, cached_before.1);
        drop(cache);
        assert!(std::fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".distd-stage-")
        }));
    }

    #[cfg(feature = "nightly-bench")]
    extern crate test;

    #[cfg(feature = "nightly-bench")]
    #[bench]
    fn bench_fs_storage(b: &mut test::Bencher) {
        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone()).unwrap();
        b.iter(|| {
            let item = new_dummy_item::<FsStorage, 1u8, 1_000_000>(&storage).unwrap();
            let stored = storage
                .get(&item.metadata.root.hash)
                .unwrap()
                .clone_data()
                .unwrap();
            assert_eq!(stored.len(), 1_000_000);
            for b in stored {
                assert_eq!(b, 1u8);
            }
        });
    }
}
