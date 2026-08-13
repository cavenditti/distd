use std::{
    cmp::Ordering,
    collections::VecDeque,
    fs::{self, create_dir_all, remove_file, File, OpenOptions},
    io::{BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::AtomicBool,
        mpsc::{sync_channel, SyncSender, TrySendError},
        Arc, Mutex, RwLock,
    },
    thread::JoinHandle,
};

use serde::{Deserialize, Serialize};
use tokio_stream::{Stream, StreamExt};

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::{
    chunk_storage::StorageError,
    chunks::{ChunkAlgorithm, ChunkInfo, CHUNK_SIZE},
    durable_state::{
        read_bounded_file, read_enveloped_state, replace_enveloped_state,
        replace_enveloped_state_with_hook, DurableReplaceStage, DurableStateKind,
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
const DEFAULT_IMMUTABLE_READER_ENTRIES: usize = 32;
const FS_STORAGE_PAYLOAD_SCHEMA: u16 = 1;
const FS_STORAGE_ROOT_DOMAIN: &[u8] = b"distd fs storage root identity v1\0";
const FS_STORAGE_REVISION_NAMESPACE: &str = ".distd-revisions-v1";
const FS_STORAGE_REVISION_MARKER_DOMAIN: &[u8] = b"distd fs server revision marker v1\0";
const FS_STORAGE_REVISION_MARKER_EXTENSION: &str = "item";
const FS_STORAGE_MAX_LINK_RECORDS: usize = 65_536;
const FS_STORAGE_MAX_LINK_DEPTH: usize = 256;
const FS_ACTIVATION_NAMESPACE: &str = ".distd-activation-v1";
const FS_ACTIVATION_ACTIVE_SCHEMA: u16 = 1;
const FS_ACTIVATION_TRANSACTION_SCHEMA: u16 = 2;
const FS_ACTIVATION_DESTINATION_DOMAIN: &[u8] = b"distd activation destination v1\0";
const FS_ACTIVATION_REVISION_DOMAIN: &[u8] = b"distd activation revision v1\0";
const FS_ACTIVATION_MAX_RECORDS: usize = 65_536;
const FS_ACTIVATION_COPY_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActiveRevisionKind {
    File,
    Directory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationRecordKind {
    Active,
    Transaction(ActivationTransactionPhase),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationTransactionPhase {
    Intent,
    Ready,
    Switched,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationOperation {
    NamespaceCreate,
    NamespaceParentSync,
    StagingCreate,
    StagingParentSync,
    RevisionDigestCreate,
    RevisionDigestParentSync,
    MaterializeDirectoryCreate,
    MaterializeFileCreate,
    MaterializeFileWrite,
    MaterializeFileSync,
    MaterializeDirectorySync,
    RevisionRename,
    RevisionParentSync,
    MirrorDirectoryCreate,
    MirrorEntryCreate,
    MirrorEntryWrite,
    MirrorEntrySync,
    MirrorEntryRename,
    MirrorEntryRemove,
    MirrorDirectorySync,
    RecoveryMirrorCleanup,
    RecoveryMirrorCleanupParentSync,
    RecoveryStagingCleanup,
    RecoveryStagingCleanupParentSync,
    RecoveryTransactionCleanup,
    RecoveryTransactionCleanupParentSync,
    RecoveryRecordTempCleanup,
    RecoveryRecordTempCleanupParentSync,
    RecoveryRecordQuarantine,
    RecoveryRecordSourceParentSync,
    RecoveryRecordQuarantineParentSync,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationFaultStage {
    BeforeIntent,
    DurableRecord {
        record: ActivationRecordKind,
        stage: DurableReplaceStage,
    },
    Operation(ActivationOperation),
    AfterIntent,
    AfterMaterialize,
    AfterRevisionPublish,
    AfterReady,
    AfterSelector,
    AfterSwitched,
    AfterStatePersist,
    AfterMirror,
}

#[derive(Clone)]
struct ActivationFaultHook {
    hook: Arc<dyn Fn(ActivationFaultStage) -> std::io::Result<()> + Send + Sync + 'static>,
    #[cfg(test)]
    simulates_crash: bool,
}

impl std::fmt::Debug for ActivationFaultHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ActivationFaultHook(..)")
    }
}

/// A snapshot of one immutable activated revision.
///
/// Handles never consult the mutable destination mirror after opening, so all
/// entry reads remain pinned to the same revision across later activations.
#[derive(Clone, Debug)]
pub struct ActiveRevisionHandle {
    active: Arc<FsActivationActiveV1>,
    revision_root: PathBuf,
    kind: ActiveRevisionKind,
}

impl ActiveRevisionHandle {
    #[must_use]
    pub fn item(&self) -> &Item {
        &self.active.item
    }

    #[must_use]
    pub fn destination(&self) -> &Path {
        &self.active.destination
    }

    #[must_use]
    pub fn kind(&self) -> ActiveRevisionKind {
        self.kind
    }

    pub fn open_file(&self) -> Result<File, Error> {
        if self.kind != ActiveRevisionKind::File {
            return Err(invalid_state("active revision is a directory"));
        }
        File::open(&self.revision_root).map_err(Into::into)
    }

    pub fn open_entry(&self, relative_path: &Path) -> Result<File, Error> {
        if self.kind != ActiveRevisionKind::Directory {
            return Err(invalid_state("active revision is a file"));
        }
        let relative_path = crate::utils::path::validate_relative(relative_path)?;
        let path = self.revision_root.join(relative_path);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid_state("active revision entry is not a regular file"));
        }
        File::open(path).map_err(Into::into)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FsActivationActiveV1 {
    root_identity: [u8; 32],
    destination: PathBuf,
    destination_identity: [u8; 32],
    revision_identity: [u8; 32],
    item: Item,
    kind: u8,
    seals: Vec<FsActivationFileSealV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FsActivationFileSealV1 {
    relative_path: Option<PathBuf>,
    size: u64,
    device: u64,
    inode: u64,
    mode: u32,
    modified_seconds: i64,
    modified_nanoseconds: i64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum FsActivationPhaseV2 {
    Intent,
    Ready,
    Switched,
}

impl From<FsActivationPhaseV2> for ActivationTransactionPhase {
    fn from(phase: FsActivationPhaseV2) -> Self {
        match phase {
            FsActivationPhaseV2::Intent => Self::Intent,
            FsActivationPhaseV2::Ready => Self::Ready,
            FsActivationPhaseV2::Switched => Self::Switched,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FsActivationTransactionV2 {
    root_identity: [u8; 32],
    transaction_id: [u8; 16],
    destination: PathBuf,
    destination_identity: [u8; 32],
    candidate_revision_identity: [u8; 32],
    previous: Option<FsActivationActiveV1>,
    item: Item,
    kind: u8,
    phase: FsActivationPhaseV2,
    candidate_seals: Vec<FsActivationFileSealV1>,
    mirror_staging_path: PathBuf,
    mirror_backup_path: PathBuf,
}

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

    fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Owned(data) => Arc::try_unwrap(data).unwrap_or_else(|data| data.as_ref().clone()),
            Self::Shared(data) => data.into_vec(),
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

#[derive(Debug)]
struct ImmutableReaderCache {
    max_entries: usize,
    order: VecDeque<PathBuf>,
    entries: HashMap<PathBuf, Arc<File>>,
}

impl ImmutableReaderCache {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            order: VecDeque::new(),
            entries: HashMap::default(),
        }
    }

    fn get_or_open(&mut self, path: &Path) -> Result<Arc<File>, Error> {
        if let Some(file) = self.entries.get(path).cloned() {
            self.order.push_back(path.to_path_buf());
            return Ok(file);
        }
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid_state(
                "immutable read backing is not a regular file",
            ));
        }
        let file = Arc::new(File::open(path)?);
        if self.max_entries == 0 {
            return Ok(file);
        }
        self.entries.insert(path.to_path_buf(), file.clone());
        self.order.push_back(path.to_path_buf());
        while self.entries.len() > self.max_entries {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if self.entries.remove(&oldest).is_some() {
                self.order.retain(|queued| queued != &oldest);
            }
        }
        Ok(file)
    }
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

fn activation_indeterminate(error: impl std::fmt::Display) -> Error {
    Error::Other(format!(
        "activation commit indeterminate; restart required: {error}"
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

fn verify_populated_chunk_hashes(
    data: &HashMap<Hash, Vec<Arc<InFileChunk>>>,
    activation_root: Option<&Path>,
) -> Result<(), Error> {
    for (hash, entries) in data {
        for entry in entries {
            if entry.populated.load(std::sync::atomic::Ordering::Relaxed)
                && !activation_root.is_some_and(|root| entry.path.starts_with(root))
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
            if normalized.components().next().is_some_and(|component| {
                component.as_os_str() == FS_STORAGE_REVISION_NAMESPACE
                    || component.as_os_str() == FS_ACTIVATION_NAMESPACE
            }) {
                return Err(invalid_state(
                    "item entry path uses a reserved storage namespace",
                ));
            }
            if !normalized_paths.insert(normalized.clone()) {
                return Err(invalid_state(
                    "item entry paths alias after canonical normalization",
                ));
            }
            if let Some(previous) = previous_path.as_ref() {
                if previous >= &normalized {
                    return Err(invalid_state(
                        "item entry paths are duplicated or not strictly ordered",
                    ));
                }
                if normalized.starts_with(previous) {
                    return Err(invalid_state(
                        "item entry path conflicts with an ancestor file",
                    ));
                }
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
        selected: &[Arc<FsActivationActiveV1>],
    ) -> Result<FsStorageState, Error> {
        if self.root_identity != fs_storage_root_identity(&root)? {
            return Err(invalid_state(
                "root identity does not match constructor root",
            ));
        }
        require_canonical_order(&self.items)?;
        require_canonical_order(&self.data)?;
        require_canonical_order(&self.links)?;

        let claimed_destinations = selected
            .iter()
            .map(|active| active.destination.clone())
            .collect::<HashSet<_>>();
        let mut claimed_roots = Vec::new();
        let mut items = HashSet::default();
        for mut item in self.items.drain(..) {
            validate_item_paths(&root, &mut item)?;
            if claimed_destinations.contains(&item.metadata.path) {
                claimed_roots.push(item.metadata.root.hash);
                continue;
            }
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
                let relative_path = crate::utils::path::validate_relative(&chunk.relative_path)?;
                if chunk.populated
                    && selected.iter().any(|active| {
                        relative_path == active.destination
                            || (active.kind == 1 && relative_path.starts_with(&active.destination))
                    })
                {
                    continue;
                }
                let path = validate_existing_chunk_path(
                    &root,
                    &relative_path,
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
            if chunks.is_empty() {
                continue;
            }
            if data.insert(entry.hash, chunks).is_some() {
                return Err(invalid_state("duplicate chunk-map hash"));
            }
        }
        for active in selected {
            let revision_root = root
                .join(FS_ACTIVATION_NAMESPACE)
                .join("revisions")
                .join(FsStorage::hex_identity(&active.revision_identity))
                .join("root");
            if active.kind == 0 {
                let mut offset = 0u64;
                for chunk in &active.item.chunks {
                    data.entry(chunk.hash)
                        .or_default()
                        .push(Arc::new(InFileChunk {
                            info: *chunk,
                            path: revision_root.clone(),
                            offset,
                            populated: Arc::new(AtomicBool::new(true)),
                        }));
                    offset = offset
                        .checked_add(chunk.size)
                        .ok_or_else(|| invalid_state("active checkpoint offset overflows"))?;
                }
            } else {
                for entry in &active.item.manifest.entries {
                    let path = revision_root.join(crate::utils::path::validate_relative(
                        Path::new(&entry.relative_path),
                    )?);
                    let mut offset = 0u64;
                    for chunk in &active.item.chunks
                        [entry.chunk_range.0 as usize..entry.chunk_range.1 as usize]
                    {
                        data.entry(chunk.hash)
                            .or_default()
                            .push(Arc::new(InFileChunk {
                                info: *chunk,
                                path: path.clone(),
                                offset,
                                populated: Arc::new(AtomicBool::new(true)),
                            }));
                        offset = offset.checked_add(chunk.size).ok_or_else(|| {
                            invalid_state("active checkpoint entry offset overflows")
                        })?;
                    }
                }
            }
        }
        verify_populated_chunk_hashes(
            &data,
            Some(&root.join(FS_ACTIVATION_NAMESPACE).join("revisions")),
        )?;
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
        // A selected activation supersedes the checkpoint's mutable-path item. Drop
        // only the link subgraph reachable exclusively from those claimed items;
        // unrelated (including orphaned) link records remain subject to strict
        // validation below and therefore cannot be hidden by a valid selector.
        let retained_roots = items.iter().map(|item| item.metadata.root.hash);
        let retained_links = FsStorageState::reachable_link_hashes(retained_roots, &links);
        let claimed_links = FsStorageState::reachable_link_hashes(claimed_roots, &links);
        links.retain(|hash, _| !claimed_links.contains(hash) || retained_links.contains(hash));
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
        verify_populated_chunk_hashes(&runtime.data, None)?;
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
        let retained = Self::reachable_link_hashes(
            self.items.iter().map(|item| item.metadata.root.hash),
            &self.links,
        );
        self.links.retain(|hash, _| retained.contains(hash));
    }

    fn reachable_link_hashes(
        roots: impl IntoIterator<Item = Hash>,
        links: &HashMap<Hash, Arc<Node>>,
    ) -> HashSet<Hash> {
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
        for root_hash in roots {
            if let Some(root) = links.get(&root_hash) {
                visit(root, links, &mut retained, 0);
            }
        }
        retained
    }

    fn reconstruct_item_links(
        &mut self,
        item: &Item,
        expected_root: ChunkInfo,
    ) -> Result<(), Error> {
        let mut partials = item
            .chunks
            .iter()
            .map(|chunk| {
                Arc::new(Node::Skipped {
                    hash: chunk.hash,
                    size: chunk.size,
                })
            })
            .collect::<Vec<_>>();
        if partials.is_empty() {
            return Err(invalid_state("active revision has no chunks"));
        }
        while partials.len() > 1 {
            let count = partials.len();
            for (to, index) in (0..count - 1).step_by(2).enumerate() {
                let left = partials[index].clone();
                let right = partials[index + 1].clone();
                let hash = merge_hashes(left.hash(), right.hash());
                let size = left
                    .size()
                    .checked_add(right.size())
                    .ok_or_else(|| invalid_state("active revision link size overflows"))?;
                let node = if let Some(existing) = self.links.get(&hash) {
                    match existing.as_ref() {
                        Node::Parent {
                            size: existing_size,
                            left: existing_left,
                            right: existing_right,
                            ..
                        } if *existing_size == size
                            && existing_left.hash() == left.hash()
                            && existing_left.size() == left.size()
                            && existing_right.hash() == right.hash()
                            && existing_right.size() == right.size() =>
                        {
                            existing.clone()
                        }
                        _ => {
                            return Err(Error::Storage(StorageError::ImmutableConflict));
                        }
                    }
                } else {
                    let node = Arc::new(Node::Parent {
                        hash,
                        size,
                        left,
                        right,
                    });
                    self.links.insert(hash, node.clone());
                    self.mark_dirty();
                    node
                };
                partials[to] = node;
            }
            let half = count / 2;
            if count % 2 != 0 {
                partials.swap(half, count - 1);
                partials.truncate(half + 1);
            } else {
                partials.truncate(half);
            }
        }
        let root = partials.swap_remove(0);
        if *root.hash() != expected_root.hash || root.size() != expected_root.size {
            return Err(invalid_state(
                "active revision chunks do not reconstruct the declared root",
            ));
        }
        Ok(())
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

const ACTIVATION_CLEANUP_QUEUE_CAPACITY: usize = 4_096;

struct ActivationCleanupCoordinator {
    sender: Option<SyncSender<(PathBuf, Option<ActivationFaultHook>)>>,
    worker: Option<JoinHandle<()>>,
    fault_hook: Arc<RwLock<Option<ActivationFaultHook>>>,
    poisoned: Arc<AtomicBool>,
}

impl std::fmt::Debug for ActivationCleanupCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActivationCleanupCoordinator")
            .finish_non_exhaustive()
    }
}

impl ActivationCleanupCoordinator {
    fn new(
        fault_hook: Arc<RwLock<Option<ActivationFaultHook>>>,
        poisoned: Arc<AtomicBool>,
    ) -> Self {
        let (sender, receiver) = sync_channel::<(PathBuf, Option<ActivationFaultHook>)>(
            ACTIVATION_CLEANUP_QUEUE_CAPACITY,
        );
        let worker_poisoned = poisoned.clone();
        let worker = std::thread::Builder::new()
            .name("distd-activation-cleanup".to_string())
            .spawn(move || {
                while let Ok((path, hook)) = receiver.recv() {
                    if hook.as_ref().is_some_and(|hook| {
                        (hook.hook)(ActivationFaultStage::Operation(
                            ActivationOperation::RecoveryTransactionCleanup,
                        ))
                        .is_err()
                    }) {
                        worker_poisoned.store(true, std::sync::atomic::Ordering::Release);
                        tracing::warn!(
                            "deferred activation transaction cleanup faulted for {}",
                            path.display()
                        );
                        continue;
                    }
                    match fs::symlink_metadata(&path) {
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                        Ok(metadata)
                            if metadata.is_file() && !metadata.file_type().is_symlink() => {}
                        Ok(_) => {
                            worker_poisoned.store(true, std::sync::atomic::Ordering::Release);
                            tracing::error!(
                                "deferred activation transaction cleanup found unsafe path {}",
                                path.display()
                            );
                            continue;
                        }
                        Err(error) => {
                            worker_poisoned.store(true, std::sync::atomic::Ordering::Release);
                            tracing::error!(
                                "deferred activation transaction cleanup could not inspect {}: {error}",
                                path.display()
                            );
                            continue;
                        }
                    }
                    if let Err(error) = fs::remove_file(&path) {
                        worker_poisoned.store(true, std::sync::atomic::Ordering::Release);
                        tracing::error!(
                            "deferred activation transaction cleanup could not remove {}: {error}",
                            path.display()
                        );
                        continue;
                    }
                    let Some(parent) = path.parent() else {
                        worker_poisoned.store(true, std::sync::atomic::Ordering::Release);
                        tracing::error!("activation transaction path has no parent");
                        continue;
                    };
                    if let Err(error) = File::open(parent).and_then(|directory| directory.sync_all())
                    {
                        worker_poisoned.store(true, std::sync::atomic::Ordering::Release);
                        tracing::error!(
                            "deferred activation transaction cleanup could not sync {}: {error}",
                            parent.display()
                        );
                    }
                }
            })
            .expect("activation cleanup worker creation must succeed");
        Self {
            sender: Some(sender),
            worker: Some(worker),
            fault_hook,
            poisoned,
        }
    }

    fn schedule(&self, path: PathBuf) {
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        let hook = self.fault_hook.read().unwrap().clone();
        match sender.try_send((path, hook)) {
            Ok(()) => {}
            Err(TrySendError::Full((path, _))) => {
                self.poisoned
                    .store(true, std::sync::atomic::Ordering::Release);
                tracing::warn!(
                    "activation cleanup queue is full; recovery will clean {}",
                    path.display()
                );
            }
            Err(TrySendError::Disconnected((path, _))) => {
                self.poisoned
                    .store(true, std::sync::atomic::Ordering::Release);
                tracing::error!(
                    "activation cleanup worker stopped; recovery will clean {}",
                    path.display()
                );
            }
        }
    }
}

impl Drop for ActivationCleanupCoordinator {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                tracing::error!("activation cleanup worker panicked");
            }
        }
    }
}

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
    activation_locks: Mutex<HashMap<[u8; 32], Arc<Mutex<()>>>>,
    activation_poisoned: Arc<AtomicBool>,
    active_refs: RwLock<HashMap<[u8; 32], Arc<FsActivationActiveV1>>>,
    activation_fault_hook: Arc<RwLock<Option<ActivationFaultHook>>>,
    activation_cleanup: ActivationCleanupCoordinator,
    #[cfg(test)]
    next_activation_transaction_id: Mutex<Option<[u8; 16]>>,
    #[cfg(test)]
    simulated_crash: AtomicBool,
    handles: RwLock<HashMap<PathBuf, Arc<Mutex<Handle>>>>,
    immutable_readers: Mutex<ImmutableReaderCache>,
    chunk_cache: Mutex<HotChunkCache>,
    tree_cache: Mutex<HotNodeCache>,
}

impl Default for FsStorage {
    fn default() -> Self {
        let activation_fault_hook = Arc::new(RwLock::new(None));
        let activation_poisoned = Arc::new(AtomicBool::new(false));
        Self {
            inner: RwLock::new(FsStorageState::default()),
            persistence_lock: Mutex::new(()),
            activation_locks: Mutex::new(HashMap::default()),
            activation_poisoned: activation_poisoned.clone(),
            active_refs: RwLock::new(HashMap::default()),
            activation_fault_hook: activation_fault_hook.clone(),
            activation_cleanup: ActivationCleanupCoordinator::new(
                activation_fault_hook,
                activation_poisoned,
            ),
            #[cfg(test)]
            next_activation_transaction_id: Mutex::new(None),
            #[cfg(test)]
            simulated_crash: AtomicBool::new(false),
            handles: RwLock::new(HashMap::default()),
            immutable_readers: Mutex::new(ImmutableReaderCache::new(
                DEFAULT_IMMUTABLE_READER_ENTRIES,
            )),
            chunk_cache: Mutex::new(HotChunkCache::new(DEFAULT_CHUNK_CACHE_BYTES)),
            tree_cache: Mutex::new(HotNodeCache::new(DEFAULT_TREE_CACHE_ENTRIES)),
        }
    }
}

impl FsStorage {
    fn activation_root(&self) -> PathBuf {
        self.inner
            .read()
            .unwrap()
            .root
            .join(FS_ACTIVATION_NAMESPACE)
    }

    fn activation_destination_identity(&self, destination: &Path) -> Result<[u8; 32], Error> {
        let destination = self.canonical_activation_destination(destination)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(FS_ACTIVATION_DESTINATION_DOMAIN);
        hasher.update(&canonical_root_bytes(&destination));
        Ok(*hasher.finalize().as_bytes())
    }

    fn canonical_activation_destination(&self, destination: &Path) -> Result<PathBuf, Error> {
        if destination.is_absolute() {
            let root = self.inner.read().unwrap().root.clone();
            let relative =
                destination
                    .strip_prefix(&root)
                    .map_err(|_| InvalidParameter::Generic {
                        expected: "an activation destination beneath the configured root"
                            .to_string(),
                        got: destination.display().to_string(),
                    })?;
            crate::utils::path::validate_relative(relative)
        } else {
            crate::utils::path::validate_relative(destination)
        }
    }

    fn activation_revision_identity(item: &Item) -> Result<[u8; 32], Error> {
        super::validate_revision_metadata(item)?;
        let payload = bitcode::serialize(item).map_err(InvalidParameter::from)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(FS_ACTIVATION_REVISION_DOMAIN);
        hasher.update(&payload);
        Ok(*hasher.finalize().as_bytes())
    }

    fn hex_identity(identity: &[u8; 32]) -> String {
        let mut encoded = String::with_capacity(64);
        for byte in identity {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
        }
        encoded
    }

    fn active_ref_path(&self, identity: &[u8; 32]) -> PathBuf {
        self.activation_root()
            .join("active")
            .join(format!("{}.state", Self::hex_identity(identity)))
    }

    fn activation_revision_root(&self, identity: &[u8; 32]) -> PathBuf {
        self.activation_root()
            .join("revisions")
            .join(Self::hex_identity(identity))
            .join("root")
    }

    fn activation_transaction_path(&self, id: &[u8; 16]) -> PathBuf {
        let mut name = Self::activation_transaction_identity(id);
        name.push_str(".state");
        self.activation_root().join("transactions").join(name)
    }

    fn activation_transaction_identity(id: &[u8; 16]) -> String {
        let mut name = String::with_capacity(32);
        for byte in id {
            use std::fmt::Write as _;
            write!(&mut name, "{byte:02x}").expect("writing to String cannot fail");
        }
        name
    }

    fn activation_staging_root(&self, id: &[u8; 16]) -> PathBuf {
        let name = Self::activation_transaction_identity(id);
        self.activation_root()
            .join("staging")
            .join(name)
            .join("root")
    }

    fn activation_mirror_paths(
        &self,
        destination: &Path,
        id: &[u8; 16],
    ) -> Result<(PathBuf, PathBuf), Error> {
        let destination = self.canonical_activation_destination(destination)?;
        let parent = destination.parent().unwrap_or_else(|| Path::new(""));
        let name = destination
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or(Error::MissingData)?;
        let id = Self::activation_transaction_identity(id);
        Ok((
            parent.join(format!(".{name}.distd-activation-mirror-{id}")),
            parent.join(format!(".{name}.distd-activation-backup-{id}")),
        ))
    }

    fn ensure_activation_namespace(&self, components: &[&str]) -> Result<PathBuf, Error> {
        let mut parent = self.inner.read().unwrap().root.clone();
        for component in std::iter::once(FS_ACTIVATION_NAMESPACE).chain(components.iter().copied())
        {
            let path = parent.join(component);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => return Err(invalid_state("activation namespace is not a directory")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    match fs::create_dir(&path) {
                        Ok(()) => {
                            self.activation_fault(ActivationFaultStage::Operation(
                                ActivationOperation::NamespaceCreate,
                            ))?;
                            #[cfg(unix)]
                            File::open(&parent)?.sync_all()?;
                            self.activation_fault(ActivationFaultStage::Operation(
                                ActivationOperation::NamespaceParentSync,
                            ))?;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(error) => return Err(error.into()),
                    }
                    let metadata = fs::symlink_metadata(&path)?;
                    if !metadata.is_dir() || metadata.file_type().is_symlink() {
                        return Err(invalid_state("activation namespace creation conflicted"));
                    }
                }
                Err(error) => return Err(error.into()),
            }
            parent = path;
        }
        Ok(parent)
    }

    fn is_hex_state_name(name: &str, digits: usize) -> bool {
        name.len() == digits + ".state".len()
            && name.ends_with(".state")
            && name[..digits]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    fn is_shared_durable_temp(name: &str, digits: usize) -> bool {
        let Some(rest) = name.strip_prefix('.') else {
            return false;
        };
        let Some((canonical, suffix)) = rest.split_once(".distd-tmp-") else {
            return false;
        };
        Self::is_hex_state_name(canonical, digits)
            && !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte == b'-')
    }

    fn sync_parent(path: &Path) -> Result<(), Error> {
        #[cfg(unix)]
        File::open(path)?.sync_all()?;
        Ok(())
    }

    fn remove_activation_path_checked(path: &Path) -> Result<(), Error> {
        let parent = path.parent().ok_or(Error::MissingData)?;
        match fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                fs::remove_file(path)?;
            }
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                fs::remove_dir_all(path)?;
            }
            Ok(_) => {
                return Err(invalid_state(
                    "activation cleanup target has an unsafe type",
                ))
            }
        }
        Self::sync_parent(parent)
    }

    fn remove_recovery_path_checked(
        &self,
        path: &Path,
        after_remove: ActivationOperation,
        after_parent_sync: ActivationOperation,
    ) -> Result<(), Error> {
        let parent = path.parent().ok_or(Error::MissingData)?;
        match fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                fs::remove_file(path)?;
            }
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                fs::remove_dir_all(path)?;
            }
            Ok(_) => return Err(invalid_state("recovery cleanup target has an unsafe type")),
        }
        self.activation_fault(ActivationFaultStage::Operation(after_remove))?;
        Self::sync_parent(parent)?;
        self.activation_fault(ActivationFaultStage::Operation(after_parent_sync))
    }

    fn quarantine_activation_record(&self, path: &Path, family: &str) -> Result<(), Error> {
        let quarantine = self.ensure_activation_namespace(&["quarantine"])?;
        let name = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| invalid_state("activation record name is not UTF-8"))?;
        let target = quarantine.join(format!("{family}-{name}-{}", uuid::Uuid::new_v4()));
        fs::rename(path, &target)?;
        self.activation_fault(ActivationFaultStage::Operation(
            ActivationOperation::RecoveryRecordQuarantine,
        ))?;
        Self::sync_parent(path.parent().ok_or(Error::MissingData)?)?;
        self.activation_fault(ActivationFaultStage::Operation(
            ActivationOperation::RecoveryRecordSourceParentSync,
        ))?;
        Self::sync_parent(&quarantine)?;
        self.activation_fault(ActivationFaultStage::Operation(
            ActivationOperation::RecoveryRecordQuarantineParentSync,
        ))
    }

    fn canonical_activation_records(
        &self,
        directory: &Path,
        family: &str,
        digits: usize,
    ) -> Result<Vec<PathBuf>, Error> {
        let mut canonical = Vec::new();
        let mut total = 0usize;
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            total += 1;
            Self::validate_activation_directory_entry_count(total)?;
            let path = entry.path();
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| invalid_state("activation record name is not UTF-8"))?;
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(invalid_state("activation record is not a regular file"));
            }
            if Self::is_shared_durable_temp(&name, digits) {
                self.remove_recovery_path_checked(
                    &path,
                    ActivationOperation::RecoveryRecordTempCleanup,
                    ActivationOperation::RecoveryRecordTempCleanupParentSync,
                )?;
            } else if Self::is_hex_state_name(&name, digits) {
                canonical.push(path);
                if canonical.len() > FS_ACTIVATION_MAX_RECORDS {
                    return Err(invalid_state("too many activation records"));
                }
            } else {
                self.quarantine_activation_record(&path, family)?;
            }
        }
        canonical.sort();
        Ok(canonical)
    }

    fn validate_activation_directory_entry_count(count: usize) -> Result<(), Error> {
        if count > FS_ACTIVATION_MAX_RECORDS {
            return Err(invalid_state("too many activation directory entries"));
        }
        Ok(())
    }

    fn write_activation_record<T: Serialize>(
        &self,
        path: &Path,
        schema: u16,
        record: ActivationRecordKind,
        value: &T,
    ) -> Result<(), Error> {
        let payload = bitcode::serialize(value).map_err(InvalidParameter::from)?;
        match (schema, record) {
            (FS_ACTIVATION_ACTIVE_SCHEMA, ActivationRecordKind::Active)
            | (FS_ACTIVATION_TRANSACTION_SCHEMA, ActivationRecordKind::Transaction(_)) => {}
            _ => return Err(invalid_state("activation record kind/schema mismatch")),
        }
        let hook = self.activation_fault_hook.read().unwrap().clone();
        replace_enveloped_state_with_hook(
            path,
            DurableStateKind::FsActivation,
            schema,
            &payload,
            MAX_DURABLE_STATE_PAYLOAD,
            |stage| {
                self.invoke_activation_hook_with(
                    hook.as_ref(),
                    ActivationFaultStage::DurableRecord { record, stage },
                )
            },
        )?;
        Ok(())
    }

    fn activation_fault(&self, stage: ActivationFaultStage) -> Result<(), Error> {
        let hook = self.activation_fault_hook.read().unwrap().clone();
        self.invoke_activation_hook_with(hook.as_ref(), stage)
            .map_err(Into::into)
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn invoke_activation_hook_with(
        &self,
        hook: Option<&ActivationFaultHook>,
        stage: ActivationFaultStage,
    ) -> std::io::Result<()> {
        let result = hook.map_or(Ok(()), |hook| (hook.hook)(stage));
        #[cfg(test)]
        if result.is_err() && hook.is_some_and(|hook| hook.simulates_crash) {
            self.simulated_crash
                .store(true, std::sync::atomic::Ordering::Release);
        }
        result
    }

    #[cfg(test)]
    fn set_activation_fault_hook<F>(&self, hook: F)
    where
        F: Fn(ActivationFaultStage) -> std::io::Result<()> + Send + Sync + 'static,
    {
        self.simulated_crash
            .store(false, std::sync::atomic::Ordering::Release);
        *self.activation_fault_hook.write().unwrap() = Some(ActivationFaultHook {
            hook: Arc::new(hook),
            simulates_crash: true,
        });
    }

    #[cfg(test)]
    fn set_activation_error_hook<F>(&self, hook: F)
    where
        F: Fn(ActivationFaultStage) -> std::io::Result<()> + Send + Sync + 'static,
    {
        self.simulated_crash
            .store(false, std::sync::atomic::Ordering::Release);
        *self.activation_fault_hook.write().unwrap() = Some(ActivationFaultHook {
            hook: Arc::new(hook),
            simulates_crash: false,
        });
    }

    #[cfg(test)]
    fn clear_activation_fault_hook(&self) {
        *self.activation_fault_hook.write().unwrap() = None;
        self.simulated_crash
            .store(false, std::sync::atomic::Ordering::Release);
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn simulated_crash(&self) -> bool {
        #[cfg(test)]
        {
            return self
                .simulated_crash
                .load(std::sync::atomic::Ordering::Acquire);
        }
        #[cfg(not(test))]
        false
    }

    fn new_activation_transaction_id(&self) -> [u8; 16] {
        #[cfg(test)]
        if let Some(id) = self.next_activation_transaction_id.lock().unwrap().take() {
            return id;
        }
        #[cfg(not(test))]
        let _ = self;
        *uuid::Uuid::new_v4().as_bytes()
    }

    #[cfg(test)]
    fn set_next_activation_transaction_id(&self, id: [u8; 16]) {
        *self.next_activation_transaction_id.lock().unwrap() = Some(id);
    }

    fn decode_active_ref(&self, path: &Path) -> Result<FsActivationActiveV1, Error> {
        let payload = read_enveloped_state(
            path,
            DurableStateKind::FsActivation,
            FS_ACTIVATION_ACTIVE_SCHEMA,
            MAX_DURABLE_STATE_PAYLOAD,
        )?;
        let active: FsActivationActiveV1 =
            bitcode::deserialize(&payload).map_err(InvalidParameter::from)?;
        self.validate_active_ref(path, &active)?;
        Ok(active)
    }

    fn validate_active_ref(&self, path: &Path, active: &FsActivationActiveV1) -> Result<(), Error> {
        if active.root_identity != fs_storage_root_identity(&self.inner.read().unwrap().root)? {
            return Err(invalid_state("activation root identity mismatch"));
        }
        let kind = Self::decode_activation_kind(active.kind)?;
        let expected_kind = if active.item.manifest.entries.is_empty() {
            ActiveRevisionKind::File
        } else {
            ActiveRevisionKind::Directory
        };
        if active.destination != crate::utils::path::validate_relative(&active.destination)?
            || active.item.metadata.path != active.destination
            || kind != expected_kind
            || active.destination_identity
                != self.activation_destination_identity(&active.destination)?
            || active.revision_identity != Self::activation_revision_identity(&active.item)?
            || self.active_ref_path(&active.destination_identity) != path
        {
            return Err(invalid_state("active reference identity mismatch"));
        }
        let root = self.activation_revision_root(&active.revision_identity);
        Self::validate_immutable_activation_shape(&root, &active.item, kind)?;
        Self::validate_activation_seals(&root, &active.item, kind, &active.seals)
    }

    fn decode_activation_kind(encoded: u8) -> Result<ActiveRevisionKind, Error> {
        match encoded {
            0 => Ok(ActiveRevisionKind::File),
            1 => Ok(ActiveRevisionKind::Directory),
            _ => Err(invalid_state("unknown active revision kind")),
        }
    }

    fn validate_immutable_activation_shape(
        root: &Path,
        item: &Item,
        kind: ActiveRevisionKind,
    ) -> Result<(), Error> {
        let metadata = fs::symlink_metadata(root)?;
        match kind {
            ActiveRevisionKind::File
                if !metadata.is_file() || metadata.file_type().is_symlink() =>
            {
                Err(invalid_state("immutable activation root is not a file"))
            }
            ActiveRevisionKind::Directory
                if !metadata.is_dir() || metadata.file_type().is_symlink() =>
            {
                Err(invalid_state(
                    "immutable activation root is not a directory",
                ))
            }
            ActiveRevisionKind::File => {
                if metadata.len() != item.size() {
                    return Err(invalid_state("immutable activation file size mismatch"));
                }
                Ok(())
            }
            ActiveRevisionKind::Directory => {
                let mut expected_files = HashMap::default();
                let mut expected_directories = HashSet::default();
                expected_directories.insert(PathBuf::new());
                for entry in &item.manifest.entries {
                    let relative =
                        crate::utils::path::validate_relative(Path::new(&entry.relative_path))?;
                    if expected_files
                        .insert(relative.clone(), entry.size)
                        .is_some()
                    {
                        return Err(invalid_state("duplicate immutable activation entry"));
                    }
                    let mut parent = relative.parent();
                    while let Some(path) = parent {
                        expected_directories.insert(path.to_path_buf());
                        parent = path.parent();
                    }
                }
                let mut directories = vec![root.to_path_buf()];
                let mut found_files = 0usize;
                let mut index = 0usize;
                while index < directories.len() {
                    for entry in fs::read_dir(&directories[index])? {
                        let entry = entry?;
                        let path = entry.path();
                        let relative = path
                            .strip_prefix(root)
                            .map_err(|_| invalid_state("immutable activation escaped root"))?;
                        let metadata = fs::symlink_metadata(&path)?;
                        if metadata.file_type().is_symlink() {
                            return Err(invalid_state("immutable activation contains a symlink"));
                        }
                        if metadata.is_dir() {
                            if !expected_directories.contains(relative) {
                                return Err(invalid_state(
                                    "immutable activation contains an unexpected directory",
                                ));
                            }
                            directories.push(path);
                        } else if metadata.is_file() {
                            let expected = expected_files.get(relative).ok_or_else(|| {
                                invalid_state("immutable activation contains an unexpected file")
                            })?;
                            if metadata.len() != *expected {
                                return Err(invalid_state(
                                    "immutable activation entry size mismatch",
                                ));
                            }
                            found_files += 1;
                        } else {
                            return Err(invalid_state(
                                "immutable activation contains a special file",
                            ));
                        }
                    }
                    index += 1;
                }
                if found_files != expected_files.len() {
                    return Err(invalid_state("immutable activation entry is missing"));
                }
                Ok(())
            }
        }
    }

    fn activation_file_seal(
        path: &Path,
        relative_path: Option<PathBuf>,
    ) -> Result<FsActivationFileSealV1, Error> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid_state(
                "activation seal target is not a regular file",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(FsActivationFileSealV1 {
                relative_path,
                size: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
                mode: metadata.mode(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
            })
        }
        #[cfg(not(unix))]
        {
            let modified = metadata
                .modified()?
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map_err(|_| invalid_state("activation seal mtime predates Unix epoch"))?;
            Ok(FsActivationFileSealV1 {
                relative_path,
                size: metadata.len(),
                device: 0,
                inode: 0,
                mode: u32::from(metadata.permissions().readonly()),
                modified_seconds: i64::try_from(modified.as_secs())
                    .map_err(InvalidParameter::from)?,
                modified_nanoseconds: i64::from(modified.subsec_nanos()),
            })
        }
    }

    fn collect_activation_seals(
        root: &Path,
        item: &Item,
        kind: ActiveRevisionKind,
    ) -> Result<Vec<FsActivationFileSealV1>, Error> {
        match kind {
            ActiveRevisionKind::File => Ok(vec![Self::activation_file_seal(root, None)?]),
            ActiveRevisionKind::Directory => item
                .manifest
                .entries
                .iter()
                .map(|entry| {
                    let relative =
                        crate::utils::path::validate_relative(Path::new(&entry.relative_path))?;
                    Self::activation_file_seal(&root.join(&relative), Some(relative))
                })
                .collect(),
        }
    }

    fn validate_activation_seals(
        root: &Path,
        item: &Item,
        kind: ActiveRevisionKind,
        seals: &[FsActivationFileSealV1],
    ) -> Result<(), Error> {
        let current = Self::collect_activation_seals(root, item, kind)?;
        if current != seals {
            Self::verify_immutable_activation_tree(root, item, kind)?;
            return Err(invalid_state(
                "activation verification seal changed; durable reactivation is required",
            ));
        }
        Ok(())
    }

    fn encode_activation_kind(kind: ActiveRevisionKind) -> u8 {
        match kind {
            ActiveRevisionKind::File => 0,
            ActiveRevisionKind::Directory => 1,
        }
    }

    fn handle_from_active(
        &self,
        active: Arc<FsActivationActiveV1>,
    ) -> Result<ActiveRevisionHandle, Error> {
        let kind = Self::decode_activation_kind(active.kind)?;
        Ok(ActiveRevisionHandle {
            revision_root: self.activation_revision_root(&active.revision_identity),
            active,
            kind,
        })
    }

    fn load_active_refs(&self) -> Result<(), Error> {
        let active_directory = self.activation_root().join("active");
        match fs::symlink_metadata(&active_directory) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.active_refs.write().unwrap().clear();
                return Ok(());
            }
            Err(error) => return Err(error.into()),
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(invalid_state("activation active path is not a directory")),
        }
        let paths = self.canonical_activation_records(&active_directory, "active", 64)?;
        let mut loaded = HashMap::default();
        for path in paths {
            let active = Arc::new(self.decode_active_ref(&path)?);
            if loaded.insert(active.destination_identity, active).is_some() {
                return Err(invalid_state("duplicate activation destination record"));
            }
        }
        *self.active_refs.write().unwrap() = loaded;
        Ok(())
    }

    fn validate_activation_transaction(
        &self,
        path: &Path,
        transaction: &FsActivationTransactionV2,
    ) -> Result<ActiveRevisionKind, Error> {
        let destination = crate::utils::path::validate_relative(&transaction.destination)?;
        let kind = Self::decode_activation_kind(transaction.kind)?;
        let expected_kind = if transaction.item.manifest.entries.is_empty() {
            ActiveRevisionKind::File
        } else {
            ActiveRevisionKind::Directory
        };
        let (expected_staging, expected_backup) =
            self.activation_mirror_paths(&transaction.destination, &transaction.transaction_id)?;
        if transaction.root_identity != fs_storage_root_identity(&self.inner.read().unwrap().root)?
            || destination != transaction.destination
            || transaction.item.metadata.path != transaction.destination
            || transaction.destination_identity
                != self.activation_destination_identity(&transaction.destination)?
            || transaction.candidate_revision_identity
                != Self::activation_revision_identity(&transaction.item)?
            || self.activation_transaction_path(&transaction.transaction_id) != path
            || kind != expected_kind
            || transaction.mirror_staging_path != expected_staging
            || transaction.mirror_backup_path != expected_backup
        {
            return Err(invalid_state("activation transaction identity mismatch"));
        }
        self.validate_owned_mirror_path(&transaction.mirror_staging_path)?;
        self.validate_owned_mirror_path(&transaction.mirror_backup_path)?;
        if let Some(previous) = transaction.previous.as_ref() {
            self.validate_active_ref(
                &self.active_ref_path(&transaction.destination_identity),
                previous,
            )?;
            if previous.destination_identity != transaction.destination_identity
                || previous.destination != transaction.destination
                || previous.item.metadata.artifact_id != transaction.item.metadata.artifact_id
                || previous.item.metadata.revision >= transaction.item.metadata.revision
            {
                return Err(invalid_state(
                    "activation transaction prior revision is inconsistent",
                ));
            }
        }
        match transaction.phase {
            FsActivationPhaseV2::Intent if !transaction.candidate_seals.is_empty() => {
                return Err(invalid_state(
                    "intent activation transaction unexpectedly contains verification seals",
                ));
            }
            FsActivationPhaseV2::Ready | FsActivationPhaseV2::Switched => {
                Self::validate_activation_seals(
                    &self.activation_revision_root(&transaction.candidate_revision_identity),
                    &transaction.item,
                    kind,
                    &transaction.candidate_seals,
                )?;
            }
            FsActivationPhaseV2::Intent => {}
        }
        Ok(kind)
    }

    fn validate_owned_mirror_path(&self, relative: &Path) -> Result<(), Error> {
        let relative = crate::utils::path::validate_relative(relative)?;
        let path = self.path(&relative)?;
        match fs::symlink_metadata(path) {
            Ok(metadata)
                if !metadata.file_type().is_symlink()
                    && (metadata.is_file() || metadata.is_dir()) =>
            {
                Ok(())
            }
            Ok(_) => Err(invalid_state(
                "activation-owned mirror path has an unsafe type",
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn preflight_owned_mirror_paths(
        &self,
        transaction: &FsActivationTransactionV2,
    ) -> Result<(), Error> {
        for relative in [
            &transaction.mirror_staging_path,
            &transaction.mirror_backup_path,
        ] {
            let path = self.path(relative)?;
            match fs::symlink_metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
                Ok(_) => {
                    return Err(invalid_state(
                        "activation-owned mirror path already exists before selector switch",
                    ))
                }
            }
        }
        Ok(())
    }

    fn validate_activation_backing_coverage(&self) -> Result<(), Error> {
        let revisions = self.activation_root().join("revisions");
        let selected_roots = self
            .active_refs
            .read()
            .unwrap()
            .values()
            .map(|active| self.activation_revision_root(&active.revision_identity))
            .collect::<Vec<_>>();
        for entries in self.inner.read().unwrap().data.values() {
            for entry in entries {
                if entry.path.starts_with(&revisions)
                    && !selected_roots
                        .iter()
                        .any(|selected| entry.path.starts_with(selected))
                {
                    return Err(invalid_state(
                        "filesystem state references an unselected activation revision",
                    ));
                }
            }
        }
        Ok(())
    }

    fn recover_activations(&self) -> Result<(), Error> {
        let root = self.activation_root();
        match fs::symlink_metadata(&root) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(invalid_state("activation namespace is not a directory")),
        }
        self.load_active_refs()?;
        let selected = self
            .active_refs
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for active in &selected {
            self.reconcile_active_item(active.as_ref(), None, false)?;
        }
        self.prune_unselected_activation_backings()?;
        let transactions = root.join("transactions");
        match fs::symlink_metadata(&transactions) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.persist_derived_activation_checkpoint()?;
                return self.validate_activation_backing_coverage();
            }
            Err(error) => return Err(error.into()),
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(invalid_state("activation transactions path is invalid")),
        }
        let paths = self.canonical_activation_records(&transactions, "transaction", 32)?;
        for path in paths {
            let payload = read_enveloped_state(
                &path,
                DurableStateKind::FsActivation,
                FS_ACTIVATION_TRANSACTION_SCHEMA,
                MAX_DURABLE_STATE_PAYLOAD,
            )?;
            let transaction: FsActivationTransactionV2 =
                bitcode::deserialize(&payload).map_err(InvalidParameter::from)?;
            self.validate_activation_transaction(&path, &transaction)?;
            let selected = self
                .active_refs
                .read()
                .unwrap()
                .get(&transaction.destination_identity)
                .cloned();
            let candidate_selected = selected.as_ref().is_some_and(|active| {
                active.revision_identity == transaction.candidate_revision_identity
            });
            if transaction.phase == FsActivationPhaseV2::Switched && !candidate_selected {
                return Err(invalid_state(
                    "activation transaction phase disagrees with selector",
                ));
            }
            if candidate_selected {
                let active = selected.expect("checked Some");
                if active.item != transaction.item
                    || active.kind != transaction.kind
                    || (transaction.phase != FsActivationPhaseV2::Intent
                        && active.seals != transaction.candidate_seals)
                {
                    return Err(invalid_state(
                        "selected activation differs from its transaction",
                    ));
                }
                self.reconcile_active_item(active.as_ref(), transaction.previous.as_ref(), false)?;
                let handle = self.handle_from_active(active)?;
                let destination = self.path(&transaction.destination)?;
                self.replace_compatibility_mirror(
                    &handle.revision_root,
                    &destination,
                    handle.kind,
                    &transaction,
                    true,
                )?;
            }
            for relative in [
                &transaction.mirror_staging_path,
                &transaction.mirror_backup_path,
            ] {
                self.remove_recovery_path_checked(
                    &self.path(relative)?,
                    ActivationOperation::RecoveryMirrorCleanup,
                    ActivationOperation::RecoveryMirrorCleanupParentSync,
                )?;
            }
            let staging_parent = self
                .activation_staging_root(&transaction.transaction_id)
                .parent()
                .ok_or(Error::MissingData)?
                .to_path_buf();
            self.remove_recovery_path_checked(
                &staging_parent,
                ActivationOperation::RecoveryStagingCleanup,
                ActivationOperation::RecoveryStagingCleanupParentSync,
            )?;
            self.remove_recovery_path_checked(
                &path,
                ActivationOperation::RecoveryTransactionCleanup,
                ActivationOperation::RecoveryTransactionCleanupParentSync,
            )?;
        }
        self.persist_derived_activation_checkpoint()?;
        self.validate_activation_backing_coverage()
    }

    fn prune_unselected_activation_backings(&self) -> Result<(), Error> {
        let revisions = self.activation_root().join("revisions");
        let selected_roots = self
            .active_refs
            .read()
            .unwrap()
            .values()
            .map(|active| self.activation_revision_root(&active.revision_identity))
            .collect::<Vec<_>>();
        let mut inner = self.inner.write().unwrap();
        let mut changed = false;
        for entries in inner.data.values_mut() {
            let before = entries.len();
            entries.retain(|entry| {
                !entry.path.starts_with(&revisions)
                    || selected_roots
                        .iter()
                        .any(|selected| entry.path.starts_with(selected))
            });
            changed |= entries.len() != before;
        }
        let before = inner.data.len();
        inner.data.retain(|_, entries| !entries.is_empty());
        changed |= inner.data.len() != before;
        if changed {
            inner.mark_dirty();
        }
        Ok(())
    }

    fn persist_derived_activation_checkpoint(&self) -> Result<(), Error> {
        if self.inner.read().unwrap().dirty {
            self.persist()?;
        }
        Ok(())
    }

    fn destination_lock(&self, identity: [u8; 32]) -> Arc<Mutex<()>> {
        self.activation_locks
            .lock()
            .unwrap()
            .entry(identity)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

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

    fn ensure_revision_namespace(&self, leaf: &str) -> Result<PathBuf, Error> {
        if leaf != "chunks" && leaf != "items" {
            return Err(invalid_state("unknown reserved revision namespace"));
        }
        let root = self.inner.read().unwrap().root.clone();
        let root_metadata = fs::symlink_metadata(&root)?;
        if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
            return Err(invalid_state(format!(
                "storage root {} is not a regular directory",
                root.display()
            )));
        }
        let mut parent = root;
        for component in [FS_STORAGE_REVISION_NAMESPACE, leaf] {
            let path = parent.join(component);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => {
                    return Err(invalid_state(format!(
                        "reserved namespace {} is not a regular directory",
                        path.display()
                    )))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    match fs::create_dir(&path) {
                        Ok(()) => {
                            #[cfg(unix)]
                            File::open(&parent)?.sync_all()?;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(error) => return Err(error.into()),
                    }
                    let metadata = fs::symlink_metadata(&path)?;
                    if !metadata.is_dir() || metadata.file_type().is_symlink() {
                        return Err(invalid_state(format!(
                            "created reserved namespace {} is not a regular directory",
                            path.display()
                        )));
                    }
                }
                Err(error) => return Err(error.into()),
            }
            parent = path;
        }
        Ok(parent)
    }

    fn has_cas_backing(inner: &FsStorageState, item: &Item) -> bool {
        !item.chunks.is_empty()
            && item.chunks.iter().all(|chunk| {
                let expected_path = inner
                    .root
                    .join(FS_STORAGE_REVISION_NAMESPACE)
                    .join("chunks")
                    .join(format!("{}.chunk", chunk.hash));
                inner.data.get(&chunk.hash).is_some_and(|backings| {
                    backings.iter().any(|backing| {
                        backing.info == *chunk
                            && backing.path == expected_path
                            && backing.offset == 0
                            && backing.populated.load(std::sync::atomic::Ordering::Relaxed)
                    })
                })
            })
    }

    fn revision_marker(&self, item: &Item) -> Result<(PathBuf, Vec<u8>), Error> {
        let payload = bitcode::serialize(item).map_err(InvalidParameter::from)?;
        if payload.len() as u64 > MAX_DURABLE_STATE_PAYLOAD {
            return Err(invalid_state("server revision marker exceeds size limit"));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(FS_STORAGE_REVISION_MARKER_DOMAIN);
        hasher.update(&payload);
        let identity = hasher.finalize();
        let path = self.path(
            &Path::new(FS_STORAGE_REVISION_NAMESPACE)
                .join("items")
                .join(format!("{identity}.{FS_STORAGE_REVISION_MARKER_EXTENSION}")),
        )?;
        Ok((path, payload))
    }

    fn verify_revision_marker(path: &Path, expected: &[u8]) -> Result<(), Error> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid_state(format!(
                "server revision marker {} is not a regular file",
                path.display()
            )));
        }
        let actual = read_bounded_file(path, MAX_DURABLE_STATE_PAYLOAD)?;
        if actual != expected {
            return Err(invalid_state(format!(
                "server revision marker {} conflicts with its Item identity",
                path.display()
            )));
        }
        Ok(())
    }

    fn commit_revision_marker(&self, item: &Item) -> Result<PathBuf, Error> {
        let (path, payload) = self.revision_marker(item)?;
        let parent = self.ensure_revision_namespace("items")?;
        if path.parent() != Some(parent.as_path()) {
            return Err(invalid_state("revision marker escaped its namespace"));
        }
        match fs::symlink_metadata(&path) {
            Ok(_) => Self::verify_revision_marker(&path, &payload)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let staging = parent.join(format!(
                    ".{}.{}.tmp",
                    path.file_stem()
                        .and_then(std::ffi::OsStr::to_str)
                        .ok_or_else(|| invalid_state("revision marker name is not UTF-8"))?,
                    uuid::Uuid::new_v4()
                ));
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&staging)?;
                if let Err(error) = file.write_all(&payload).and_then(|()| file.sync_all()) {
                    drop(file);
                    let _ = remove_file(&staging);
                    return Err(error.into());
                }
                drop(file);
                match fs::hard_link(&staging, &path) {
                    Ok(()) => remove_file(&staging)?,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        remove_file(&staging)?;
                        Self::verify_revision_marker(&path, &payload)?;
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
        Ok(path)
    }

    fn remove_revision_marker(path: &Path) -> Result<(), Error> {
        match remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        #[cfg(unix)]
        File::open(path.parent().ok_or(Error::MissingData)?)?.sync_all()?;
        Ok(())
    }

    fn cleanup_preselector_activation(
        &self,
        staging_parent: &Path,
        transaction_path: &Path,
    ) -> Result<(), Error> {
        let staging_result = Self::remove_activation_path_checked(staging_parent);
        let transaction_result = Self::remove_revision_marker(transaction_path);
        match (staging_result, transaction_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(staging), Ok(())) => Err(invalid_state(format!(
                "pre-selector staging cleanup failed: {staging}"
            ))),
            (Ok(()), Err(transaction)) => Err(invalid_state(format!(
                "pre-selector transaction cleanup failed: {transaction}"
            ))),
            (Err(staging), Err(transaction)) => Err(invalid_state(format!(
                "pre-selector staging cleanup failed ({staging}); transaction cleanup also failed ({transaction})"
            ))),
        }
    }

    fn read_revision_markers(&self) -> Result<Vec<(PathBuf, Item)>, Error> {
        let directory = self.path(&Path::new(FS_STORAGE_REVISION_NAMESPACE).join("items"))?;
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(invalid_state(format!(
                    "server revision marker namespace {} is not a directory",
                    directory.display()
                )))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        }
        let mut markers = Vec::new();
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let file_name = entry
                .file_name()
                .into_string()
                .map_err(|_| invalid_state("server revision marker name is not UTF-8"))?;
            if file_name.starts_with('.') && file_name.ends_with(".tmp") {
                continue;
            }
            if Path::new(&file_name)
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                != Some(FS_STORAGE_REVISION_MARKER_EXTENSION)
            {
                return Err(invalid_state(format!(
                    "unexpected server revision marker entry {file_name}"
                )));
            }
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(invalid_state(format!(
                    "server revision marker {} is not a regular file",
                    path.display()
                )));
            }
            let payload = read_bounded_file(&path, MAX_DURABLE_STATE_PAYLOAD)?;
            let item: Item = bitcode::deserialize(&payload).map_err(InvalidParameter::from)?;
            super::validate_revision_metadata(&item)?;
            let (expected_path, expected_payload) = self.revision_marker(&item)?;
            if path != expected_path || payload != expected_payload {
                return Err(invalid_state(format!(
                    "server revision marker {} has the wrong content-addressed identity",
                    path.display()
                )));
            }
            Self::verify_revision_marker(&path, &payload)?;
            markers.push((path, item));
        }
        Ok(markers)
    }

    fn restore_items_after_failed_cleanup(&self, items: Vec<Item>) {
        let mut inner = self.inner.write().unwrap();
        for item in items {
            inner.insert_item(item);
        }
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
        let parent = self.ensure_revision_namespace("chunks")?;
        if path.parent() != Some(parent.as_path()) {
            return Err(invalid_state("immutable chunk escaped its namespace"));
        }
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
        let (marker_path, marker_payload) = self.revision_marker(&item)?;
        let marker_exists = match fs::symlink_metadata(&marker_path) {
            Ok(_) => {
                Self::verify_revision_marker(&marker_path, &marker_payload)?;
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        let item_exists = self.inner.read().unwrap().items.contains(&item);
        if item_exists && !marker_exists {
            return Err(invalid_state(
                "exact existing Item lacks immutable server-revision provenance",
            ));
        }
        let marker_created = !marker_exists;
        if marker_created {
            self.commit_revision_marker(&item)?;
        }
        self.inner.write().unwrap().insert_item(item.clone());
        if let Err(error) = self.persist() {
            if !item_exists {
                self.inner.write().unwrap().remove_item(&item);
            }
            let commit_indeterminate = matches!(
                &error,
                Error::DurableState(
                    crate::durable_state::DurableStateError::CommitIndeterminate { .. }
                )
            );
            if marker_created && !commit_indeterminate {
                if let Err(cleanup) = Self::remove_revision_marker(&marker_path) {
                    return Err(invalid_state(format!(
                        "revision state persist failed ({error}); marker cleanup also failed ({cleanup})"
                    )));
                }
            }
            return Err(error);
        }
        Ok(item)
    }

    fn migrate_referenced_chunks_to_cas(&self) -> Result<bool, Error> {
        let selected_active_items = self
            .active_refs
            .read()
            .unwrap()
            .values()
            .map(|active| active.item.clone())
            .collect::<HashSet<_>>();
        let referenced = self
            .inner
            .read()
            .unwrap()
            .items
            .iter()
            .filter(|item| !selected_active_items.contains(*item))
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

    fn is_immutable_backing(&self, path: &Path) -> bool {
        let root = self.inner.read().unwrap().root.clone();
        path.starts_with(root.join(FS_ACTIVATION_NAMESPACE).join("revisions"))
            || path.starts_with(root.join(FS_STORAGE_REVISION_NAMESPACE).join("chunks"))
    }

    fn read_immutable_backing(
        &self,
        path: &Path,
        offset: u64,
        bytes: &mut [u8],
    ) -> Result<(), Error> {
        let file = self.immutable_readers.lock().unwrap().get_or_open(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_exact_at(bytes, offset)?;
        }
        #[cfg(not(unix))]
        {
            let mut file = file.try_clone()?;
            file.seek(std::io::SeekFrom::Start(offset))?;
            file.read_exact(bytes)?;
        }
        Ok(())
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

        if self.is_immutable_backing(&entry.path)
            && self
                .read_immutable_backing(&entry.path, entry.offset, &mut buf)
                .is_ok()
        {
            return Some(ChunkData::Owned(
                self.cache_chunk_data(*hash, Arc::new(buf)),
            ));
        }

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
                    if !self
                        .inner
                        .read()
                        .unwrap()
                        .data
                        .contains_key(&chunk_info.hash)
                    {
                        return Err(Error::Storage(StorageError::TreeReconstruct));
                    }
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

    fn materialize_activation_tree(
        &self,
        staging_root: &Path,
        kind: ActiveRevisionKind,
        manifest: &Manifest,
        chunks: &[ChunkInfo],
        staged_data: &HashMap<Hash, ChunkData>,
        previous: Option<&FsActivationActiveV1>,
    ) -> Result<(), Error> {
        if kind == ActiveRevisionKind::File
            && self.materialize_incremental_activation_file(
                staging_root,
                chunks,
                staged_data,
                previous,
            )?
        {
            return Ok(());
        }
        if kind == ActiveRevisionKind::Directory {
            fs::create_dir(staging_root)?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MaterializeDirectoryCreate,
            ))?;
        }
        let entries = if manifest.entries.is_empty() {
            vec![(staging_root.to_path_buf(), 0usize, chunks.len(), None)]
        } else {
            manifest
                .entries
                .iter()
                .map(|entry| {
                    let relative =
                        crate::utils::path::validate_relative(Path::new(&entry.relative_path))?;
                    Ok((
                        staging_root.join(relative),
                        entry.chunk_range.0 as usize,
                        entry.chunk_range.1 as usize,
                        Some(entry),
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?
        };
        for (path, start, end, entry) in entries {
            if start > end || end > chunks.len() {
                return Err(Error::Storage(StorageError::TreeReconstruct));
            }
            if let Some(parent) = path.parent() {
                let parent_existed = parent.exists();
                create_dir_all(parent)?;
                if !parent_existed {
                    self.activation_fault(ActivationFaultStage::Operation(
                        ActivationOperation::MaterializeDirectoryCreate,
                    ))?;
                }
            }
            if let (Some(previous), Some(entry)) = (previous, entry) {
                if let Some(previous_entry) =
                    previous.item.manifest.entries.iter().find(|candidate| {
                        candidate.relative_path == entry.relative_path
                            && candidate.size == entry.size
                            && previous.item.chunks
                                [candidate.chunk_range.0 as usize..candidate.chunk_range.1 as usize]
                                == chunks[start..end]
                    })
                {
                    fs::hard_link(
                        self.activation_revision_root(&previous.revision_identity)
                            .join(&previous_entry.relative_path),
                        &path,
                    )?;
                    self.activation_fault(ActivationFaultStage::Operation(
                        ActivationOperation::MaterializeFileCreate,
                    ))?;
                    continue;
                }
            }
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MaterializeFileCreate,
            ))?;
            for chunk in &chunks[start..end] {
                if let Some(data) = staged_data.get(&chunk.hash) {
                    if data.as_ref().len() as u64 != chunk.size
                        || do_hash(data.as_ref()) != chunk.hash
                    {
                        return Err(Error::Storage(StorageError::TreeReconstruct));
                    }
                    file.write_all(data.as_ref())?;
                } else {
                    self.copy_existing_chunk_bounded(chunk, &mut file)?;
                }
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MaterializeFileWrite,
                ))?;
            }
            file.sync_all()?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MaterializeFileSync,
            ))?;
        }
        if kind == ActiveRevisionKind::Directory {
            self.sync_activation_directory_tree(
                staging_root,
                ActivationOperation::MaterializeDirectorySync,
            )?;
        }
        Ok(())
    }

    fn materialize_incremental_activation_file(
        &self,
        staging_root: &Path,
        chunks: &[ChunkInfo],
        staged_data: &HashMap<Hash, ChunkData>,
        previous: Option<&FsActivationActiveV1>,
    ) -> Result<bool, Error> {
        let Some(previous) = previous.filter(|previous| {
            previous.kind == Self::encode_activation_kind(ActiveRevisionKind::File)
                && previous.item.manifest.entries.is_empty()
                && previous.seals.len() == 1
                && previous.seals[0].relative_path.is_none()
        }) else {
            return Ok(false);
        };
        let previous_root = self.activation_revision_root(&previous.revision_identity);
        if !Self::try_clone_file(&previous_root, staging_root)? {
            match fs::symlink_metadata(staging_root) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    Self::remove_activation_path_checked(staging_root)?;
                }
                Ok(_) => {
                    return Err(invalid_state(
                        "failed activation clone left an unsafe staging path",
                    ))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(false);
        }
        self.activation_fault(ActivationFaultStage::Operation(
            ActivationOperation::MaterializeFileCreate,
        ))?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(staging_root)?;
        let mut candidate_offset = 0u64;
        let mut previous_offset = 0u64;
        for (index, chunk) in chunks.iter().enumerate() {
            let previous_chunk = previous.item.chunks.get(index);
            let unchanged = previous_chunk == Some(chunk) && previous_offset == candidate_offset;
            if !unchanged {
                file.seek(std::io::SeekFrom::Start(candidate_offset))?;
                if let Some(data) = staged_data.get(&chunk.hash) {
                    if data.as_ref().len() as u64 != chunk.size
                        || do_hash(data.as_ref()) != chunk.hash
                    {
                        return Err(Error::Storage(StorageError::TreeReconstruct));
                    }
                    file.write_all(data.as_ref())?;
                } else {
                    self.copy_existing_chunk_bounded(chunk, &mut file)?;
                }
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MaterializeFileWrite,
                ))?;
            }
            candidate_offset = candidate_offset
                .checked_add(chunk.size)
                .ok_or_else(|| invalid_state("activation candidate file size overflows"))?;
            if let Some(previous_chunk) = previous_chunk {
                previous_offset = previous_offset
                    .checked_add(previous_chunk.size)
                    .ok_or_else(|| invalid_state("prior activation file size overflows"))?;
            }
        }
        file.set_len(candidate_offset)?;
        file.sync_all()?;
        self.activation_fault(ActivationFaultStage::Operation(
            ActivationOperation::MaterializeFileSync,
        ))?;
        Ok(true)
    }

    fn copy_existing_chunk_bounded(
        &self,
        chunk: &ChunkInfo,
        output: &mut File,
    ) -> Result<(), Error> {
        let backing = self
            .inner
            .read()
            .unwrap()
            .data
            .get(&chunk.hash)
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| {
                        entry.info == *chunk
                            && entry.populated.load(std::sync::atomic::Ordering::Relaxed)
                    })
                    .cloned()
            })
            .ok_or(Error::Storage(StorageError::TreeReconstruct))?;
        let mut input = File::open(&backing.path)?;
        input.seek(std::io::SeekFrom::Start(backing.offset))?;
        let mut remaining = chunk.size;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0u8; FS_ACTIVATION_COPY_BUFFER_BYTES];
        while remaining > 0 {
            let length = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(InvalidParameter::from)?;
            input.read_exact(&mut buffer[..length])?;
            hasher.update(&buffer[..length]);
            output.write_all(&buffer[..length])?;
            remaining -= length as u64;
        }
        if Hash::from(hasher.finalize()) != chunk.hash {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }
        Ok(())
    }

    fn sync_directory_tree_with<F>(root: &Path, mut after_sync: F) -> Result<(), Error>
    where
        F: FnMut() -> Result<(), Error>,
    {
        let mut directories = vec![root.to_path_buf()];
        let mut index = 0usize;
        while index < directories.len() {
            for entry in fs::read_dir(&directories[index])? {
                let entry = entry?;
                let metadata = fs::symlink_metadata(entry.path())?;
                if metadata.is_dir() && !metadata.file_type().is_symlink() {
                    directories.push(entry.path());
                }
            }
            index += 1;
        }
        for directory in directories.into_iter().rev() {
            File::open(directory)?.sync_all()?;
            after_sync()?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn sync_directory_tree(root: &Path) -> Result<(), Error> {
        Self::sync_directory_tree_with(root, || Ok(()))
    }

    fn sync_activation_directory_tree(
        &self,
        root: &Path,
        operation: ActivationOperation,
    ) -> Result<(), Error> {
        Self::sync_directory_tree_with(root, || {
            self.activation_fault(ActivationFaultStage::Operation(operation))
        })
    }

    fn preflight_compatibility_mirror(
        &self,
        destination: &Path,
        item: &Item,
        kind: ActiveRevisionKind,
    ) -> Result<(), Error> {
        let storage_root = self.inner.read().unwrap().root.clone();
        let relative = destination
            .strip_prefix(&storage_root)
            .map_err(|_| invalid_state("activation destination escaped storage root"))?;
        let mut parent = storage_root.clone();
        let components = relative.components().collect::<Vec<_>>();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            let std::path::Component::Normal(component) = component else {
                return Err(invalid_state("activation destination is not canonical"));
            };
            let path = parent.join(component);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => {
                    return Err(invalid_state(
                        "activation destination parent is not a regular directory",
                    ))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&path)?;
                    Self::sync_parent(&parent)?;
                }
                Err(error) => return Err(error.into()),
            }
            parent = path;
        }
        let mut destination_is_directory = false;
        match fs::symlink_metadata(destination) {
            Ok(metadata)
                if !metadata.file_type().is_symlink()
                    && (metadata.is_file() || metadata.is_dir()) =>
            {
                if metadata.is_dir() {
                    destination_is_directory = true;
                    let mut directories = vec![destination.to_path_buf()];
                    let mut index = 0usize;
                    while index < directories.len() {
                        for entry in fs::read_dir(&directories[index])? {
                            let entry = entry?;
                            let metadata = fs::symlink_metadata(entry.path())?;
                            if metadata.file_type().is_symlink()
                                || (!metadata.is_file() && !metadata.is_dir())
                            {
                                return Err(invalid_state(
                                    "activation destination contains an unsafe entry",
                                ));
                            }
                            if metadata.is_dir() {
                                directories.push(entry.path());
                            }
                        }
                        index += 1;
                    }
                }
            }
            Ok(_) => return Err(invalid_state("activation destination has an unsafe type")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if kind == ActiveRevisionKind::Directory && destination_is_directory {
            for entry in &item.manifest.entries {
                let relative =
                    crate::utils::path::validate_relative(Path::new(&entry.relative_path))?;
                let mut current = destination.to_path_buf();
                for component in relative.components() {
                    current.push(component.as_os_str());
                    match fs::symlink_metadata(&current) {
                        Ok(metadata)
                            if !metadata.file_type().is_symlink()
                                && (metadata.is_file() || metadata.is_dir()) => {}
                        Ok(_) => {
                            return Err(invalid_state(
                                "activation destination entry has an unsafe type",
                            ))
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
        Ok(())
    }

    fn ensure_activation_digest_directory(&self, revision_root: &Path) -> Result<(), Error> {
        let directory = revision_root.parent().ok_or(Error::MissingData)?;
        let parent = directory.parent().ok_or(Error::MissingData)?;
        match fs::symlink_metadata(directory) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(invalid_state("activation revision digest path is unsafe")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(directory) {
                    Ok(()) => {
                        self.activation_fault(ActivationFaultStage::Operation(
                            ActivationOperation::RevisionDigestCreate,
                        ))?;
                        Self::sync_parent(parent)?;
                        self.activation_fault(ActivationFaultStage::Operation(
                            ActivationOperation::RevisionDigestParentSync,
                        ))?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let metadata = fs::symlink_metadata(directory)?;
                        if !metadata.is_dir() || metadata.file_type().is_symlink() {
                            return Err(invalid_state(
                                "activation revision digest creation conflicted",
                            ));
                        }
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let staging_parent = self.activation_root().join("staging");
            if fs::metadata(directory)?.dev() != fs::metadata(staging_parent)?.dev() {
                return Err(invalid_state(
                    "activation staging and revision namespaces are on different devices",
                ));
            }
        }
        Ok(())
    }

    fn verify_immutable_activation_tree(
        root: &Path,
        item: &Item,
        kind: ActiveRevisionKind,
    ) -> Result<(), Error> {
        let metadata = fs::symlink_metadata(root)?;
        match kind {
            ActiveRevisionKind::File
                if !metadata.is_file() || metadata.file_type().is_symlink() =>
            {
                return Err(invalid_state("immutable activation root is not a file"));
            }
            ActiveRevisionKind::Directory
                if !metadata.is_dir() || metadata.file_type().is_symlink() =>
            {
                return Err(invalid_state(
                    "immutable activation root is not a directory",
                ));
            }
            _ => {}
        }
        let entries = if item.manifest.entries.is_empty() {
            vec![(root.to_path_buf(), 0usize, item.chunks.len())]
        } else {
            item.manifest
                .entries
                .iter()
                .map(|entry| {
                    Ok((
                        root.join(crate::utils::path::validate_relative(Path::new(
                            &entry.relative_path,
                        ))?),
                        entry.chunk_range.0 as usize,
                        entry.chunk_range.1 as usize,
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?
        };
        for (path, start, end) in entries {
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(invalid_state("immutable activation entry is not a file"));
            }
            let mut file = File::open(&path)?;
            for chunk in item.chunks.get(start..end).ok_or_else(|| {
                invalid_state("immutable activation entry has invalid chunk range")
            })? {
                let mut bytes =
                    vec![0u8; usize::try_from(chunk.size).map_err(InvalidParameter::from)?];
                file.read_exact(&mut bytes)?;
                if do_hash(&bytes) != chunk.hash {
                    return Err(invalid_state("immutable activation chunk hash mismatch"));
                }
            }
            let expected = item.chunks[start..end]
                .iter()
                .map(|chunk| chunk.size)
                .sum::<u64>();
            if metadata.len() != expected {
                return Err(invalid_state("immutable activation entry size mismatch"));
            }
        }
        Ok(())
    }

    fn replace_compatibility_mirror(
        &self,
        revision_root: &Path,
        destination: &Path,
        kind: ActiveRevisionKind,
        transaction: &FsActivationTransactionV2,
        recovery: bool,
    ) -> Result<(), Error> {
        let parent = destination.parent().ok_or(Error::MissingData)?;
        create_dir_all(parent)?;
        let staging = self.path(&transaction.mirror_staging_path)?;
        let backup = self.path(&transaction.mirror_backup_path)?;
        self.validate_owned_mirror_path(&transaction.mirror_staging_path)?;
        self.validate_owned_mirror_path(&transaction.mirror_backup_path)?;
        let result = match kind {
            ActiveRevisionKind::File => {
                self.remove_mirror_owned_path(&staging, recovery)?;
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorEntryRemove,
                ))?;
                Self::clone_or_copy_file(revision_root, &staging)?;
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorEntryCreate,
                ))?;
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorEntryWrite,
                ))?;
                File::open(&staging)?.sync_all()?;
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorEntrySync,
                ))?;
                self.atomic_replace_owned(&staging, destination, &backup, None)
            }
            ActiveRevisionKind::Directory => {
                self.update_directory_mirror_incrementally(revision_root, destination, transaction)
            }
        };
        result?;
        self.remove_mirror_owned_path(&staging, recovery)?;
        self.activation_fault(ActivationFaultStage::Operation(
            ActivationOperation::MirrorEntryRemove,
        ))?;
        self.remove_mirror_owned_path(&backup, recovery)?;
        self.activation_fault(ActivationFaultStage::Operation(
            ActivationOperation::MirrorEntryRemove,
        ))
    }

    fn remove_mirror_owned_path(&self, path: &Path, recovery: bool) -> Result<(), Error> {
        if recovery {
            self.remove_recovery_path_checked(
                path,
                ActivationOperation::RecoveryMirrorCleanup,
                ActivationOperation::RecoveryMirrorCleanupParentSync,
            )
        } else {
            Self::remove_activation_path_checked(path)
        }
    }

    fn try_clone_file(source: &Path, destination: &Path) -> Result<bool, Error> {
        #[cfg(target_os = "macos")]
        {
            use std::{ffi::CString, os::unix::ffi::OsStrExt};

            unsafe extern "C" {
                fn clonefile(
                    source: *const std::ffi::c_char,
                    destination: *const std::ffi::c_char,
                    flags: u32,
                ) -> std::ffi::c_int;
            }

            let source = CString::new(source.as_os_str().as_bytes())
                .map_err(|_| invalid_state("activation clone source contains NUL"))?;
            let destination_c = CString::new(destination.as_os_str().as_bytes())
                .map_err(|_| invalid_state("activation clone destination contains NUL"))?;
            // SAFETY: both pointers are valid NUL-terminated path strings for the duration of
            // the call, and destination is an activation-owned unique staging path.
            if unsafe { clonefile(source.as_ptr(), destination_c.as_ptr(), 0) } == 0 {
                return Ok(true);
            }
            match fs::symlink_metadata(destination) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    fs::remove_file(destination)?;
                }
                Ok(_) => {
                    return Err(invalid_state(
                        "failed activation clone left an unsafe staging path",
                    ))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(false)
    }

    fn clone_or_copy_file(source: &Path, destination: &Path) -> Result<(), Error> {
        if Self::try_clone_file(source, destination)? {
            return Ok(());
        }
        fs::copy(source, destination)?;
        Ok(())
    }

    fn update_directory_mirror_incrementally(
        &self,
        revision_root: &Path,
        destination: &Path,
        transaction: &FsActivationTransactionV2,
    ) -> Result<(), Error> {
        let previous = transaction.previous.as_ref();
        let item = &transaction.item;
        let staging_root = self.path(&transaction.mirror_staging_path)?;
        let backup_root = self.path(&transaction.mirror_backup_path)?;
        let mut changed_directories = HashSet::default();
        match fs::symlink_metadata(destination) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                match fs::symlink_metadata(&backup_root) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        fs::rename(destination, &backup_root)?;
                        changed_directories.insert(
                            destination
                                .parent()
                                .ok_or(Error::MissingData)?
                                .to_path_buf(),
                        );
                    }
                    Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {}
                    Ok(_) => {
                        return Err(invalid_state("activation mirror backup has the wrong type"))
                    }
                    Err(error) => return Err(error.into()),
                }
                fs::create_dir(destination)?;
                changed_directories.insert(
                    destination
                        .parent()
                        .ok_or(Error::MissingData)?
                        .to_path_buf(),
                );
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorDirectoryCreate,
                ))?;
            }
            Ok(_) => return Err(invalid_state("compatibility mirror has an unsafe type")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(destination)?;
                changed_directories.insert(
                    destination
                        .parent()
                        .ok_or(Error::MissingData)?
                        .to_path_buf(),
                );
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorDirectoryCreate,
                ))?;
            }
            Err(error) => return Err(error.into()),
        }
        match fs::symlink_metadata(&staging_root) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(invalid_state(
                    "activation mirror staging has the wrong type",
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&staging_root)?;
                Self::sync_parent(staging_root.parent().ok_or(Error::MissingData)?)?;
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorDirectoryCreate,
                ))?;
            }
            Err(error) => return Err(error.into()),
        }

        let mut expected_files = HashMap::default();
        let mut expected_directories = HashSet::default();
        expected_directories.insert(PathBuf::new());
        for entry in &item.manifest.entries {
            let relative = crate::utils::path::validate_relative(Path::new(&entry.relative_path))?;
            expected_files.insert(relative.clone(), entry);
            let mut parent = relative.parent();
            while let Some(path) = parent {
                expected_directories.insert(path.to_path_buf());
                parent = path.parent();
            }
        }

        let mut directories = vec![destination.to_path_buf()];
        let mut index = 0usize;
        while index < directories.len() {
            for entry in fs::read_dir(&directories[index])? {
                let entry = entry?;
                let path = entry.path();
                let relative = path
                    .strip_prefix(destination)
                    .map_err(|_| invalid_state("compatibility mirror escaped destination"))?;
                let metadata = fs::symlink_metadata(&path)?;
                if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir())
                {
                    return Err(invalid_state(
                        "compatibility mirror contains an unsafe entry",
                    ));
                }
                if metadata.is_dir() {
                    directories.push(path);
                } else if !expected_files.contains_key(relative) {
                    let backup = backup_root.join(relative);
                    if let Some(parent) = backup.parent() {
                        create_dir_all(parent)?;
                    }
                    match fs::symlink_metadata(&backup) {
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            fs::rename(&path, &backup)?;
                            changed_directories
                                .insert(path.parent().ok_or(Error::MissingData)?.to_path_buf());
                            self.activation_fault(ActivationFaultStage::Operation(
                                ActivationOperation::MirrorEntryRemove,
                            ))?;
                        }
                        Ok(metadata)
                            if metadata.is_file() && !metadata.file_type().is_symlink() =>
                        {
                            Self::remove_activation_path_checked(&path)?;
                            changed_directories
                                .insert(path.parent().ok_or(Error::MissingData)?.to_path_buf());
                            self.activation_fault(ActivationFaultStage::Operation(
                                ActivationOperation::MirrorEntryRemove,
                            ))?;
                        }
                        Ok(_) => {
                            return Err(invalid_state(
                                "activation mirror removed-entry backup has an unsafe type",
                            ))
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            index += 1;
        }
        for directory in directories.into_iter().skip(1).rev() {
            let relative = directory
                .strip_prefix(destination)
                .map_err(|_| invalid_state("compatibility mirror escaped destination"))?;
            if !expected_directories.contains(relative) {
                Self::remove_activation_path_checked(&directory)?;
                changed_directories
                    .insert(directory.parent().ok_or(Error::MissingData)?.to_path_buf());
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorEntryRemove,
                ))?;
            }
        }

        for entry in &item.manifest.entries {
            let relative = crate::utils::path::validate_relative(Path::new(&entry.relative_path))?;
            let target = destination.join(&relative);
            Self::ensure_mirror_parent_directories(
                destination,
                relative.parent(),
                &mut changed_directories,
            )?;
            let unchanged = previous.is_some_and(|previous| {
                Self::decode_activation_kind(previous.kind).ok()
                    == Some(ActiveRevisionKind::Directory)
                    && previous.item.manifest.entries.iter().any(|candidate| {
                        candidate.relative_path == entry.relative_path
                            && candidate.size == entry.size
                            && previous.item.chunks
                                [candidate.chunk_range.0 as usize..candidate.chunk_range.1 as usize]
                                == item.chunks
                                    [entry.chunk_range.0 as usize..entry.chunk_range.1 as usize]
                    })
            });
            if unchanged
                && fs::symlink_metadata(&target).is_ok_and(|metadata| {
                    metadata.is_file()
                        && !metadata.file_type().is_symlink()
                        && metadata.len() == entry.size
                })
            {
                continue;
            }
            let source = revision_root.join(&relative);
            let staging = staging_root.join(&relative);
            Self::ensure_mirror_parent_directories(
                &staging_root,
                relative.parent(),
                &mut changed_directories,
            )?;
            Self::remove_activation_path_checked(&staging)?;
            Self::clone_or_copy_file(&source, &staging)?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MirrorEntryCreate,
            ))?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MirrorEntryWrite,
            ))?;
            File::open(&staging)?.sync_all()?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MirrorEntrySync,
            ))?;
            let backup = backup_root.join(&relative);
            self.atomic_replace_owned(&staging, &target, &backup, Some(&mut changed_directories))?;
        }
        let mut changed_directories = changed_directories.into_iter().collect::<Vec<_>>();
        changed_directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for directory in changed_directories {
            File::open(directory)?.sync_all()?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MirrorDirectorySync,
            ))?;
        }
        Ok(())
    }

    fn ensure_mirror_parent_directories(
        root: &Path,
        relative: Option<&Path>,
        changed_directories: &mut HashSet<PathBuf>,
    ) -> Result<(), Error> {
        let Some(relative) = relative else {
            return Ok(());
        };
        let mut parent = root.to_path_buf();
        for component in relative.components() {
            let std::path::Component::Normal(component) = component else {
                return Err(invalid_state("invalid compatibility mirror directory"));
            };
            let path = parent.join(component);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => {
                    return Err(invalid_state(
                        "compatibility mirror parent is not a directory",
                    ))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&path)?;
                    changed_directories.insert(parent.clone());
                }
                Err(error) => return Err(error.into()),
            }
            parent = path;
        }
        Ok(())
    }

    fn activate_verified_item(
        &self,
        item: Item,
        destination: &Path,
        staged_data: &HashMap<Hash, ChunkData>,
    ) -> Result<ActiveRevisionHandle, Error> {
        if self
            .activation_poisoned
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(invalid_state(
                "activation state is indeterminate and requires restart",
            ));
        }
        let destination = self.canonical_activation_destination(destination)?;
        let destination_identity = self.activation_destination_identity(&destination)?;
        let destination_lock = self.destination_lock(destination_identity);
        let _destination_guard = destination_lock.lock().unwrap();
        if self
            .activation_poisoned
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(invalid_state(
                "activation state is indeterminate and requires restart",
            ));
        }
        let revision_identity = Self::activation_revision_identity(&item)?;
        let kind = if item.manifest.entries.is_empty() {
            ActiveRevisionKind::File
        } else {
            ActiveRevisionKind::Directory
        };
        let root_identity = fs_storage_root_identity(&self.inner.read().unwrap().root)?;
        let active_path = self.active_ref_path(&destination_identity);
        let previous = match fs::symlink_metadata(&active_path) {
            Ok(_) => Some(self.decode_active_ref(&active_path)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if item.metadata.path != destination {
            return Err(invalid_state(
                "activation item path does not match its destination",
            ));
        }
        if let Some(previous) = previous.as_ref() {
            if previous.item == item {
                return self.handle_from_active(Arc::new(previous.clone()));
            }
            if previous.item.metadata.artifact_id != item.metadata.artifact_id {
                return Err(invalid_state(
                    "activation destination is owned by a different artifact",
                ));
            }
            if item.metadata.revision <= previous.item.metadata.revision {
                return Err(invalid_state(
                    "activation revision must increase strictly for an existing artifact",
                ));
            }
        }

        self.ensure_activation_namespace(&["transactions"])?;
        self.ensure_activation_namespace(&["staging"])?;
        self.ensure_activation_namespace(&["revisions"])?;
        self.ensure_activation_namespace(&["active"])?;
        self.ensure_activation_namespace(&["quarantine"])?;

        let transaction_id = self.new_activation_transaction_id();
        let transaction_path = self.activation_transaction_path(&transaction_id);
        let (mirror_staging_path, mirror_backup_path) =
            self.activation_mirror_paths(&destination, &transaction_id)?;
        let transaction = FsActivationTransactionV2 {
            root_identity,
            transaction_id,
            destination: destination.clone(),
            destination_identity,
            candidate_revision_identity: revision_identity,
            previous,
            item: item.clone(),
            kind: Self::encode_activation_kind(kind),
            phase: FsActivationPhaseV2::Intent,
            candidate_seals: Vec::new(),
            mirror_staging_path,
            mirror_backup_path,
        };
        // A durable Intent establishes ownership of these deterministic paths. Existing
        // entries must therefore be rejected before the Intent can make recovery treat
        // them as activation-owned cleanup targets.
        self.preflight_owned_mirror_paths(&transaction)?;
        self.activation_fault(ActivationFaultStage::BeforeIntent)?;
        self.write_activation_record(
            &transaction_path,
            FS_ACTIVATION_TRANSACTION_SCHEMA,
            ActivationRecordKind::Transaction(ActivationTransactionPhase::Intent),
            &transaction,
        )?;
        let staging_root = self.activation_staging_root(&transaction_id);
        let staging_parent = staging_root
            .parent()
            .ok_or(Error::MissingData)?
            .to_path_buf();
        let prepared = (|| {
            self.activation_fault(ActivationFaultStage::AfterIntent)?;
            fs::create_dir(&staging_parent)?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::StagingCreate,
            ))?;
            #[cfg(unix)]
            File::open(staging_parent.parent().ok_or(Error::MissingData)?)?.sync_all()?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::StagingParentSync,
            ))?;
            self.materialize_activation_tree(
                &staging_root,
                kind,
                &item.manifest,
                &item.chunks,
                staged_data,
                transaction.previous.as_ref(),
            )?;
            self.activation_fault(ActivationFaultStage::AfterMaterialize)?;
            let revision_root = self.activation_revision_root(&revision_identity);
            let revision_directory = revision_root.parent().ok_or(Error::MissingData)?;
            self.ensure_activation_digest_directory(&revision_root)?;
            let mut newly_published = false;
            match fs::symlink_metadata(&revision_root) {
                Ok(_) => {
                    Self::remove_activation_path_checked(&staging_parent)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if fs::read_dir(revision_directory)?.next().is_some() {
                        return Err(invalid_state(
                            "activation revision digest directory contains an unexpected entry",
                        ));
                    }
                    fs::rename(&staging_root, &revision_root)?;
                    self.activation_fault(ActivationFaultStage::Operation(
                        ActivationOperation::RevisionRename,
                    ))?;
                    Self::sync_parent(revision_directory)?;
                    self.activation_fault(ActivationFaultStage::Operation(
                        ActivationOperation::RevisionParentSync,
                    ))?;
                    Self::remove_activation_path_checked(&staging_parent)?;
                    newly_published = true;
                }
                Err(error) => return Err(error.into()),
            }
            if !newly_published {
                Self::verify_immutable_activation_tree(&revision_root, &item, kind)?;
            }
            self.activation_fault(ActivationFaultStage::AfterRevisionPublish)?;
            let candidate_seals = Self::collect_activation_seals(&revision_root, &item, kind)?;
            let destination_path = self.path(&destination)?;
            self.preflight_compatibility_mirror(&destination_path, &item, kind)?;
            self.activation_fault(ActivationFaultStage::AfterReady)?;
            let active = FsActivationActiveV1 {
                root_identity,
                destination: destination.clone(),
                destination_identity,
                revision_identity,
                item: item.clone(),
                kind: Self::encode_activation_kind(kind),
                seals: candidate_seals,
            };
            Ok::<_, Error>((revision_root, destination_path, active))
        })();
        let (revision_root, destination_path, active) = match prepared {
            Ok(prepared) => prepared,
            Err(error) if self.simulated_crash() => return Err(error),
            Err(error) => {
                if let Err(cleanup) =
                    self.cleanup_preselector_activation(&staging_parent, &transaction_path)
                {
                    self.activation_poisoned
                        .store(true, std::sync::atomic::Ordering::Release);
                    return Err(activation_indeterminate(invalid_state(format!(
                        "activation failed before selector ({error}); checked cleanup failed ({cleanup})"
                    ))));
                }
                return Err(error);
            }
        };
        if let Err(error) = self.write_activation_record(
            &active_path,
            FS_ACTIVATION_ACTIVE_SCHEMA,
            ActivationRecordKind::Active,
            &active,
        ) {
            if matches!(
                error,
                Error::DurableState(
                    crate::durable_state::DurableStateError::CommitIndeterminate { .. }
                )
            ) {
                self.activation_poisoned
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(activation_indeterminate(error));
            }
            if self.simulated_crash() {
                return Err(error);
            }
            if let Err(cleanup) =
                self.cleanup_preselector_activation(&staging_parent, &transaction_path)
            {
                self.activation_poisoned
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(activation_indeterminate(invalid_state(format!(
                    "active selector write failed ({error}); checked pre-selector cleanup failed ({cleanup})"
                ))));
            }
            return Err(error);
        }
        let active = Arc::new(active);
        self.active_refs
            .write()
            .unwrap()
            .insert(destination_identity, active.clone());

        let post_switch = (|| {
            self.activation_fault(ActivationFaultStage::AfterSelector)?;
            self.activation_fault(ActivationFaultStage::AfterSwitched)?;
            self.reconcile_active_item(active.as_ref(), transaction.previous.as_ref(), false)?;
            self.activation_fault(ActivationFaultStage::AfterStatePersist)?;
            self.replace_compatibility_mirror(
                &revision_root,
                &destination_path,
                kind,
                &transaction,
                false,
            )?;
            self.activation_fault(ActivationFaultStage::AfterMirror)?;
            Ok::<(), Error>(())
        })();
        if let Err(error) = post_switch {
            self.activation_poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(activation_indeterminate(error));
        }
        self.activation_cleanup.schedule(transaction_path);
        self.handle_from_active(active)
    }

    fn reconcile_active_item(
        &self,
        active: &FsActivationActiveV1,
        previous: Option<&FsActivationActiveV1>,
        persist_checkpoint: bool,
    ) -> Result<(), Error> {
        let kind = Self::decode_activation_kind(active.kind)?;
        let revision_root = self.activation_revision_root(&active.revision_identity);
        let destination_path = self.path(&active.destination)?;
        let previous_revision_root =
            previous.map(|previous| self.activation_revision_root(&previous.revision_identity));
        let item = &active.item;
        let expected_root = super::validate_revision_metadata(item)?;
        let mut inner = self.inner.write().unwrap();
        inner
            .items
            .retain(|existing| existing.metadata.path != item.metadata.path);
        for entries in inner.data.values_mut() {
            entries.retain(|entry| {
                !entry.path.starts_with(&destination_path)
                    && !entry.path.starts_with(&revision_root)
                    && !previous_revision_root
                        .as_ref()
                        .is_some_and(|previous| entry.path.starts_with(previous))
            });
        }
        inner.data.retain(|_, entries| !entries.is_empty());
        match kind {
            ActiveRevisionKind::File => {
                let mut offset = 0u64;
                for chunk in &item.chunks {
                    inner
                        .data
                        .entry(chunk.hash)
                        .or_default()
                        .push(Arc::new(InFileChunk {
                            info: *chunk,
                            path: revision_root.clone(),
                            offset,
                            populated: Arc::new(AtomicBool::new(true)),
                        }));
                    offset = offset
                        .checked_add(chunk.size)
                        .ok_or_else(|| invalid_state("active revision backing offset overflows"))?;
                }
            }
            ActiveRevisionKind::Directory => {
                for entry in &item.manifest.entries {
                    let path = revision_root.join(crate::utils::path::validate_relative(
                        Path::new(&entry.relative_path),
                    )?);
                    let mut offset = 0u64;
                    for chunk in
                        &item.chunks[entry.chunk_range.0 as usize..entry.chunk_range.1 as usize]
                    {
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
                        offset = offset.checked_add(chunk.size).ok_or_else(|| {
                            invalid_state("active directory backing offset overflows")
                        })?;
                    }
                }
            }
        }
        inner.reconstruct_item_links(item, expected_root)?;
        inner.insert_item(item.clone());
        inner.retain_links_reachable_from_items();
        drop(inner);
        if persist_checkpoint {
            self.persist()?;
        }
        self.handles.write().unwrap().retain(|path, _| {
            !path.starts_with(&destination_path)
                && !path.starts_with(&revision_root)
                && !previous_revision_root
                    .as_ref()
                    .is_some_and(|previous| path.starts_with(previous))
        });
        Ok(())
    }

    fn atomic_replace_owned(
        &self,
        staging: &Path,
        destination: &Path,
        backup: &Path,
        mut changed_directories: Option<&mut HashSet<PathBuf>>,
    ) -> Result<(), Error> {
        let parent = destination.parent().ok_or(Error::MissingData)?;
        let destination_metadata = match fs::symlink_metadata(destination) {
            Ok(metadata)
                if !metadata.file_type().is_symlink()
                    && (metadata.is_file() || metadata.is_dir()) =>
            {
                Some(metadata)
            }
            Ok(_) => return Err(invalid_state("replacement destination has an unsafe type")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let staging_metadata = fs::symlink_metadata(staging)?;
        if staging_metadata.file_type().is_symlink()
            || (!staging_metadata.is_file() && !staging_metadata.is_dir())
        {
            return Err(invalid_state("replacement staging path has an unsafe type"));
        }
        if destination_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.is_file())
            && staging_metadata.is_file()
            && !staging_metadata.file_type().is_symlink()
        {
            fs::rename(staging, destination)?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MirrorEntryRename,
            ))?;
            if let Some(directories) = changed_directories.as_deref_mut() {
                directories.insert(parent.to_path_buf());
            } else {
                File::open(parent)?.sync_all()?;
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorDirectorySync,
                ))?;
            }
            return Ok(());
        }
        let mut moved_destination = false;
        if destination_metadata.is_some() {
            let backup_exists = match fs::symlink_metadata(backup) {
                Ok(metadata)
                    if !metadata.file_type().is_symlink()
                        && (metadata.is_file() || metadata.is_dir()) =>
                {
                    true
                }
                Ok(_) => return Err(invalid_state("replacement backup path has an unsafe type")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            };
            if backup_exists {
                return Err(invalid_state(
                    "replacement destination and deterministic backup both exist",
                ));
            }
            if let Some(backup_parent) = backup.parent() {
                create_dir_all(backup_parent)?;
            }
            fs::rename(destination, backup)?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MirrorEntryRename,
            ))?;
            if let Some(directories) = changed_directories.as_deref_mut() {
                directories.insert(parent.to_path_buf());
            } else {
                Self::sync_parent(parent)?;
                self.activation_fault(ActivationFaultStage::Operation(
                    ActivationOperation::MirrorDirectorySync,
                ))?;
            }
            moved_destination = true;
        }

        if let Err(error) = fs::rename(staging, destination) {
            if moved_destination {
                let _ = fs::rename(backup, destination);
                let _ = Self::sync_parent(parent);
            }
            return Err(error.into());
        }
        self.activation_fault(ActivationFaultStage::Operation(
            ActivationOperation::MirrorEntryRename,
        ))?;
        if let Some(directories) = changed_directories.as_deref_mut() {
            directories.insert(parent.to_path_buf());
        } else {
            Self::sync_parent(parent)?;
            self.activation_fault(ActivationFaultStage::Operation(
                ActivationOperation::MirrorDirectorySync,
            ))?;
        }
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
        let path = self.canonical_activation_destination(&path)?;
        if chunk_infos.len() != manifest.chunk_count as usize {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }

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
                    if !self
                        .inner
                        .read()
                        .unwrap()
                        .data
                        .contains_key(&chunk_info.hash)
                    {
                        return Err(Error::Storage(StorageError::TreeReconstruct));
                    }
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

        let item = Item::make_with_entries(
            name,
            path.clone(),
            revision,
            description,
            root.chunk_info(),
            chunks,
            manifest.entries.clone(),
            manifest.chunk_algorithm,
        )?
        .with_artifact_id(manifest.artifact_id.clone());

        let _handle = self.activate_verified_item(item.clone(), &path, &staged_data)?;

        self.cache_verified_received_chunks(received_in_order);
        Ok(item)
    }

    fn persist_locked(&self) -> Result<(), Error> {
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

    fn persist(&self) -> Result<(), Error> {
        let _single_writer = self.persistence_lock.lock().unwrap();
        self.persist_locked()
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

        let activation_fault_hook = Arc::new(RwLock::new(None));
        let activation_poisoned = Arc::new(AtomicBool::new(false));
        let storage = Self {
            inner: RwLock::new(FsStorageState {
                root: root.clone(),
                persistance_path: persistance_path.clone(),
                ..Default::default()
            }),
            persistence_lock: Mutex::new(()),
            activation_locks: Mutex::new(HashMap::default()),
            activation_poisoned: activation_poisoned.clone(),
            active_refs: RwLock::new(HashMap::default()),
            activation_fault_hook: activation_fault_hook.clone(),
            activation_cleanup: ActivationCleanupCoordinator::new(
                activation_fault_hook,
                activation_poisoned,
            ),
            #[cfg(test)]
            next_activation_transaction_id: Mutex::new(None),
            #[cfg(test)]
            simulated_crash: AtomicBool::new(false),
            handles: RwLock::new(HashMap::default()),
            immutable_readers: Mutex::new(ImmutableReaderCache::new(
                DEFAULT_IMMUTABLE_READER_ENTRIES,
            )),
            chunk_cache: Mutex::new(HotChunkCache::new(cache_config.max_chunk_bytes)),
            tree_cache: Mutex::new(HotNodeCache::new(cache_config.max_tree_entries)),
        };
        // Selectors are authoritative and independently enveloped. Authenticate them before
        // decoding the derived Fs checkpoint so stale direct-path backings claimed by an exact
        // selector cannot prevent recovery, while corrupt selectors still fail closed.
        storage.load_active_refs()?;
        let selected = storage
            .active_refs
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();

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
                canonical.into_runtime(root.clone(), persistance_path.clone(), &selected)?
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
                        actual.into_runtime(root.clone(), persistance_path.clone(), &[])?
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

        *storage.inner.write().unwrap() = state;
        storage.recover_activations()?;
        if storage.migrate_referenced_chunks_to_cas()? {
            storage.persist()?;
        }
        let mut handles = HashMap::default();
        let activation_root = storage.activation_root();
        for entries in storage.inner.read().unwrap().data.values() {
            for entry in entries {
                if !entry.path.starts_with(&activation_root) && !handles.contains_key(&entry.path) {
                    handles.insert(
                        entry.path.clone(),
                        Arc::new(Mutex::new(Handle::existing(&entry.path)?)),
                    );
                }
            }
        }
        *storage.handles.write().unwrap() = handles;
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

    /// Open the immutable revision currently selected for a destination.
    pub fn open_active_revision(
        &self,
        destination: &Path,
    ) -> Result<Option<ActiveRevisionHandle>, Error> {
        if self
            .activation_poisoned
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(invalid_state(
                "activation state is indeterminate and requires restart",
            ));
        }
        let destination = self.canonical_activation_destination(destination)?;
        let identity = self.activation_destination_identity(&destination)?;
        if let Some(active) = self.active_refs.read().unwrap().get(&identity).cloned() {
            return self.handle_from_active(active).map(Some);
        }
        let destination_lock = self.destination_lock(identity);
        let destination_guard = destination_lock.lock().unwrap();
        if self
            .activation_poisoned
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(invalid_state(
                "activation state is indeterminate and requires restart",
            ));
        }
        if let Some(active) = self.active_refs.read().unwrap().get(&identity).cloned() {
            return self.handle_from_active(active).map(Some);
        }

        let Some(item) = self.item_for_path(&destination) else {
            return Ok(None);
        };
        let kind = if item.manifest.entries.is_empty() {
            ActiveRevisionKind::File
        } else {
            ActiveRevisionKind::Directory
        };
        let direct_path = self.path(&destination)?;
        Self::verify_immutable_activation_tree(&direct_path, &item, kind)?;
        drop(destination_guard);
        self.activate_verified_item(item, &destination, &HashMap::default())
            .map(Some)
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
            return self.read_chunk_data(&chunk.hash).map(ChunkData::into_vec);
        }

        let node = self.get(root)?;
        let leaves = node.flatten_iter().ok()?;
        leaves.get(index as usize).map(|arc| (**arc).clone())
    }

    fn discard_unpublished_revision(&self, item: &Item) -> Result<(), Error> {
        let _single_writer = self.persistence_lock.lock().unwrap();
        let (marker_path, marker_payload) = self.revision_marker(item)?;
        match fs::symlink_metadata(&marker_path) {
            Ok(_) => Self::verify_revision_marker(&marker_path, &marker_payload)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }

        let removed = self.inner.write().unwrap().remove_item(item);
        if removed {
            if let Err(error) = self.persist_locked() {
                self.restore_items_after_failed_cleanup(vec![item.clone()]);
                return Err(error);
            }
        }
        Self::remove_revision_marker(&marker_path)?;
        Ok(())
    }

    fn reconcile_published_revisions(&self, published: &[Item]) -> Result<(), Error> {
        let _single_writer = self.persistence_lock.lock().unwrap();
        let published = published.iter().cloned().collect::<HashSet<_>>();
        let stored_published = {
            let inner = self.inner.read().unwrap();
            inner
                .items
                .iter()
                .filter(|item| published.contains(*item) && Self::has_cas_backing(&inner, item))
                .cloned()
                .collect::<Vec<_>>()
        };
        for item in stored_published {
            self.commit_revision_marker(&item)?;
        }

        let markers = self.read_revision_markers()?;
        let mut removed = Vec::new();
        let mut orphan_markers = Vec::new();
        for (path, item) in markers {
            if !published.contains(&item) {
                orphan_markers.push(path);
                if self.inner.write().unwrap().remove_item(&item) {
                    removed.push(item);
                }
            }
        }
        if !removed.is_empty() {
            if let Err(error) = self.persist_locked() {
                self.restore_items_after_failed_cleanup(removed);
                return Err(error);
            }
        }
        for marker in orphan_markers {
            Self::remove_revision_marker(&marker)?;
        }
        Ok(())
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

    fn activation_test_item(bytes: &[u8], revision: u32) -> Item {
        use crate::chunk_storage::hashmap_storage::HashMapStorage;

        let source = HashMapStorage::default();
        let (root, chunks) = source
            .chunk_tree(bytes, ChunkAlgorithm::fixed_aligned(4, 1))
            .unwrap();
        let mut item = Item::make(
            "artifact".to_string(),
            PathBuf::from("artifact.bin"),
            revision,
            Some("activation fixture".to_string()),
            root.chunk_info(),
            chunks,
            ChunkAlgorithm::fixed_aligned(4, 1),
        )
        .unwrap();
        item.metadata.created = std::time::SystemTime::UNIX_EPOCH;
        item.metadata.updated = std::time::SystemTime::UNIX_EPOCH;
        item
    }

    fn activation_staged_data(bytes: &[u8], item: &Item) -> HashMap<Hash, ChunkData> {
        let mut offset = 0usize;
        item.chunks
            .iter()
            .map(|chunk| {
                let end = offset + chunk.size as usize;
                let data = bytes[offset..end].to_vec();
                offset = end;
                (chunk.hash, ChunkData::Owned(Arc::new(data)))
            })
            .collect()
    }

    fn activation_directory_item(first: &[u8], second: &[u8], revision: u32) -> Item {
        use crate::chunk_storage::hashmap_storage::HashMapStorage;

        let mut bytes = first.to_vec();
        bytes.extend_from_slice(second);
        let source = HashMapStorage::default();
        let (root, chunks) = source
            .chunk_tree(&bytes, ChunkAlgorithm::fixed_aligned(4, 1))
            .unwrap();
        let mut item = Item::make_with_entries(
            "directory-artifact".to_string(),
            PathBuf::from("artifact-dir"),
            revision,
            None,
            root.chunk_info(),
            chunks,
            vec![
                FileEntry {
                    relative_path: "a.bin".to_string(),
                    size: first.len() as u64,
                    chunk_range: (0, 1),
                },
                FileEntry {
                    relative_path: "b.bin".to_string(),
                    size: second.len() as u64,
                    chunk_range: (1, 2),
                },
            ],
            ChunkAlgorithm::fixed_aligned(4, 1),
        )
        .unwrap();
        item.metadata.created = std::time::SystemTime::UNIX_EPOCH;
        item.metadata.updated = std::time::SystemTime::UNIX_EPOCH;
        item
    }

    fn activation_directory_fixture(entries: &[(&str, &[u8])], revision: u32) -> (Item, Vec<u8>) {
        use crate::chunk_storage::hashmap_storage::HashMapStorage;

        let bytes = entries
            .iter()
            .flat_map(|(_, bytes)| bytes.iter().copied())
            .collect::<Vec<_>>();
        let source = HashMapStorage::default();
        let (root, chunks) = source
            .chunk_tree(&bytes, ChunkAlgorithm::fixed_aligned(4, 1))
            .unwrap();
        let file_entries = entries
            .iter()
            .enumerate()
            .map(|(index, (path, bytes))| FileEntry {
                relative_path: (*path).to_string(),
                size: bytes.len() as u64,
                chunk_range: (index as u32, index as u32 + 1),
            })
            .collect();
        let mut item = Item::make_with_entries(
            "directory-artifact".to_string(),
            PathBuf::from("artifact-dir"),
            revision,
            None,
            root.chunk_info(),
            chunks,
            file_entries,
            ChunkAlgorithm::fixed_aligned(4, 1),
        )
        .unwrap();
        item.metadata.created = std::time::SystemTime::UNIX_EPOCH;
        item.metadata.updated = std::time::SystemTime::UNIX_EPOCH;
        (item, bytes)
    }

    fn assert_directory_fixture(root: &Path, entries: &[(&str, &[u8])]) {
        let directory = root.join("artifact-dir");
        for (relative, expected) in entries {
            assert_eq!(fs::read(directory.join(relative)).unwrap(), *expected);
        }
        let mut actual = Vec::new();
        let mut pending = vec![directory.clone()];
        while let Some(parent) = pending.pop() {
            for entry in fs::read_dir(parent).unwrap() {
                let entry = entry.unwrap();
                let metadata = fs::symlink_metadata(entry.path()).unwrap();
                if metadata.is_dir() && !metadata.file_type().is_symlink() {
                    pending.push(entry.path());
                } else {
                    assert!(metadata.is_file() && !metadata.file_type().is_symlink());
                    actual.push(
                        entry
                            .path()
                            .strip_prefix(&directory)
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string(),
                    );
                }
            }
        }
        actual.sort();
        let mut expected = entries
            .iter()
            .map(|(path, _)| (*path).to_string())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(actual, expected);
    }

    fn transition_file_item(bytes: &[u8], revision: u32) -> Item {
        let mut item = activation_test_item(bytes, revision).with_artifact_id("transition".into());
        item.metadata.path = PathBuf::from("target");
        item
    }

    fn transition_directory_item(first: &[u8], second: &[u8], revision: u32) -> Item {
        let mut item = activation_directory_item(first, second, revision)
            .with_artifact_id("transition".into());
        item.metadata.path = PathBuf::from("target");
        item
    }

    fn install_switched_transaction(
        storage: &FsStorage,
        previous: FsActivationActiveV1,
        active: &FsActivationActiveV1,
        transaction_id: [u8; 16],
    ) -> FsActivationTransactionV2 {
        let (mirror_staging_path, mirror_backup_path) = storage
            .activation_mirror_paths(&active.destination, &transaction_id)
            .unwrap();
        let transaction = FsActivationTransactionV2 {
            root_identity: active.root_identity,
            transaction_id,
            destination: active.destination.clone(),
            destination_identity: active.destination_identity,
            candidate_revision_identity: active.revision_identity,
            previous: Some(previous),
            item: active.item.clone(),
            kind: active.kind,
            phase: FsActivationPhaseV2::Switched,
            candidate_seals: active.seals.clone(),
            mirror_staging_path,
            mirror_backup_path,
        };
        storage
            .write_activation_record(
                &storage.activation_transaction_path(&transaction_id),
                FS_ACTIVATION_TRANSACTION_SCHEMA,
                ActivationRecordKind::Transaction(ActivationTransactionPhase::Switched),
                &transaction,
            )
            .unwrap();
        transaction
    }

    fn hex_bytes(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        let mut result = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut result, "{byte:02x}").unwrap();
        }
        result
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
    fn discard_unpublished_revision_is_durable_retry_safe_and_keeps_cas_bytes() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let bytes = bytes::Bytes::from_static(b"unpublished immutable revision");
        let item = storage
            .prepare_revision(
                "unpublished".to_string(),
                "unpublished".to_string(),
                PathBuf::from("logical/unpublished.bin"),
                1,
                None,
                bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        let item = storage.commit_revision(item, bytes).unwrap();
        storage.verify_committed_revision(&item).unwrap();
        let cas_path = storage.revision_chunk_path(&item.chunks[0].hash).unwrap();
        let (marker_path, _) = storage.revision_marker(&item).unwrap();
        assert!(cas_path.is_file());
        assert!(marker_path.is_file());

        storage.discard_unpublished_revision(&item).unwrap();
        storage.discard_unpublished_revision(&item).unwrap();
        assert!(!storage.inner.read().unwrap().items.contains(&item));
        assert!(!marker_path.exists());
        assert!(
            cas_path.is_file(),
            "cleanup must retain immutable CAS bytes"
        );
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        assert!(!restarted.inner.read().unwrap().items.contains(&item));
        assert!(cas_path.is_file());
    }

    #[test]
    fn discard_unpublished_revision_reports_persistence_failure_and_restores_memory() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let bytes = bytes::Bytes::from_static(b"cleanup must fail explicitly");
        let item = storage
            .prepare_revision(
                "unpublished".to_string(),
                "unpublished".to_string(),
                PathBuf::from("logical/unpublished.bin"),
                1,
                None,
                bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .and_then(|item| storage.commit_revision(item, bytes))
            .unwrap();
        let original_state_path = {
            let mut inner = storage.inner.write().unwrap();
            let original = inner.persistance_path.clone();
            inner.persistance_path = root.clone();
            original
        };

        assert!(storage.discard_unpublished_revision(&item).is_err());
        assert!(storage.inner.read().unwrap().items.contains(&item));

        storage.inner.write().unwrap().persistance_path = original_state_path;
        storage.persist().unwrap();
    }

    #[test]
    fn marker_before_state_failure_is_clean_on_restart_and_retry() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let bytes = bytes::Bytes::from_static(b"state persist must fail");
        let item = storage
            .prepare_revision(
                "failed".to_string(),
                "failed".to_string(),
                PathBuf::from("same/path.bin"),
                1,
                None,
                bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        let (marker_path, _) = storage.revision_marker(&item).unwrap();
        let original_state_path = {
            let mut inner = storage.inner.write().unwrap();
            let original = inner.persistance_path.clone();
            inner.persistance_path = root.clone();
            original
        };

        assert!(storage.commit_revision(item.clone(), bytes).is_err());
        assert!(!marker_path.exists());
        assert!(!storage.inner.read().unwrap().items.contains(&item));

        storage.inner.write().unwrap().persistance_path = original_state_path;
        storage.persist().unwrap();
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        restarted.reconcile_published_revisions(&[]).unwrap();
        assert!(!restarted.inner.read().unwrap().items.contains(&item));
        let retry_bytes = bytes::Bytes::from_static(b"different bytes after restart");
        let retry = restarted
            .prepare_revision(
                "failed".to_string(),
                "failed".to_string(),
                PathBuf::from("same/path.bin"),
                1,
                None,
                retry_bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .and_then(|item| restarted.commit_revision(item, retry_bytes))
            .unwrap();
        restarted.verify_committed_revision(&retry).unwrap();
    }

    #[test]
    fn first_revision_publication_creates_verified_reserved_namespace() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let namespace = root.join(FS_STORAGE_REVISION_NAMESPACE);
        assert!(!namespace.exists());
        let bytes = bytes::Bytes::from_static(b"first publication");
        let item = storage
            .prepare_revision(
                "first".to_string(),
                "first".to_string(),
                PathBuf::from("first.bin"),
                1,
                None,
                bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .and_then(|item| storage.commit_revision(item, bytes))
            .unwrap();

        for path in [
            namespace.clone(),
            namespace.join("chunks"),
            namespace.join("items"),
        ] {
            let metadata = fs::symlink_metadata(&path).unwrap();
            assert!(metadata.is_dir());
            assert!(!metadata.file_type().is_symlink());
        }
        assert!(storage
            .revision_chunk_path(&item.chunks[0].hash)
            .unwrap()
            .is_file());
        assert!(storage.revision_marker(&item).unwrap().0.is_file());
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        restarted.verify_committed_revision(&item).unwrap();
    }

    #[test]
    fn reserved_namespace_conflicts_fail_before_first_publication() {
        let root = temp_path();
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join(FS_STORAGE_REVISION_NAMESPACE), b"conflict").unwrap();
        let storage = FsStorage::new(root.clone()).unwrap();
        let bytes = bytes::Bytes::from_static(b"must not publish");
        let item = storage
            .prepare_revision(
                "first".to_string(),
                "first".to_string(),
                PathBuf::from("first.bin"),
                1,
                None,
                bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();

        assert!(storage.commit_revision(item.clone(), bytes).is_err());
        assert!(!storage.inner.read().unwrap().items.contains(&item));
        assert_eq!(
            fs::read(root.join(FS_STORAGE_REVISION_NAMESPACE)).unwrap(),
            b"conflict"
        );
    }

    #[test]
    fn concurrent_first_publications_share_durable_namespace_creation() {
        let root = temp_path();
        let storage = Arc::new(FsStorage::new(root.clone()).unwrap());
        let workers = (0..8)
            .map(|index| {
                let storage = storage.clone();
                std::thread::spawn(move || {
                    let bytes = bytes::Bytes::from(format!("concurrent first publication {index}"));
                    let item = storage
                        .prepare_revision(
                            format!("artifact-{index}"),
                            format!("artifact-{index}"),
                            PathBuf::from(format!("artifact-{index}.bin")),
                            1,
                            None,
                            bytes.clone(),
                            ChunkAlgorithm::default(),
                        )
                        .unwrap();
                    storage.commit_revision(item, bytes).unwrap()
                })
            })
            .collect::<Vec<_>>();
        let items = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();

        for item in &items {
            storage.verify_committed_revision(item).unwrap();
            assert!(storage.revision_marker(item).unwrap().0.is_file());
        }
        drop(storage);
        let restarted = FsStorage::new(root).unwrap();
        for item in items {
            restarted.verify_committed_revision(&item).unwrap();
        }
    }

    #[test]
    fn startup_reconciliation_discards_only_orphan_revision_records() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let published_bytes = bytes::Bytes::from_static(b"published revision");
        let published = storage
            .prepare_revision(
                "published".to_string(),
                "published".to_string(),
                PathBuf::from("logical/published.bin"),
                1,
                None,
                published_bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .and_then(|item| storage.commit_revision(item, published_bytes))
            .unwrap();
        let orphan_bytes = bytes::Bytes::from_static(b"crash orphan revision");
        let orphan = storage
            .prepare_revision(
                "orphan".to_string(),
                "orphan".to_string(),
                PathBuf::from("logical/orphan.bin"),
                1,
                None,
                orphan_bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .and_then(|item| storage.commit_revision(item, orphan_bytes.clone()))
            .unwrap();
        let ordinary = storage
            .create_item(
                "ordinary".to_string(),
                PathBuf::from("logical/orphan.bin"),
                0,
                None,
                orphan_bytes,
                ChunkAlgorithm::default(),
            )
            .unwrap();
        let orphan_cas = storage.revision_chunk_path(&orphan.chunks[0].hash).unwrap();
        let (orphan_marker, _) = storage.revision_marker(&orphan).unwrap();
        let (published_marker, _) = storage.revision_marker(&published).unwrap();
        remove_file(&published_marker).unwrap();
        drop(storage);

        let restarted = FsStorage::new(root.clone()).unwrap();
        restarted
            .reconcile_published_revisions(std::slice::from_ref(&published))
            .unwrap();
        {
            let inner = restarted.inner.read().unwrap();
            assert!(inner.items.contains(&published));
            assert!(!inner.items.contains(&orphan));
            assert!(inner.items.contains(&ordinary));
        }
        assert!(orphan_cas.is_file(), "reconciliation retains CAS chunks");
        assert!(!orphan_marker.exists(), "orphan provenance is consumed");
        assert!(
            published_marker.is_file(),
            "published legacy records receive provenance markers"
        );
        drop(restarted);

        let restarted = FsStorage::new(root).unwrap();
        {
            let inner = restarted.inner.read().unwrap();
            assert!(inner.items.contains(&published));
            assert!(!inner.items.contains(&orphan));
            assert!(inner.items.contains(&ordinary));
        }
        let retry_bytes = bytes::Bytes::from_static(b"different retry bytes");
        let retry = restarted
            .prepare_revision(
                "orphan".to_string(),
                "orphan".to_string(),
                PathBuf::from("logical/orphan.bin"),
                1,
                None,
                retry_bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .and_then(|item| restarted.commit_revision(item, retry_bytes))
            .unwrap();
        restarted.verify_committed_revision(&retry).unwrap();
        assert!(restarted.inner.read().unwrap().items.contains(&ordinary));
    }

    #[test]
    fn discard_unpublished_revision_preserves_ordinary_item_records() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let ordinary = storage
            .create_item(
                "ordinary".to_string(),
                PathBuf::from("mutable/ordinary.bin"),
                0,
                None,
                bytes::Bytes::from_static(b"ordinary"),
                ChunkAlgorithm::default(),
            )
            .unwrap();

        storage.discard_unpublished_revision(&ordinary).unwrap();
        assert!(storage.inner.read().unwrap().items.contains(&ordinary));
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        assert!(restarted.inner.read().unwrap().items.contains(&ordinary));
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
            .into_runtime(root.clone(), root.join("decoded-state"), &[])
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

    #[test]
    fn active_revision_handle_survives_replacement_and_restart() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let first_bytes = b"abcdefgh".to_vec();
        let first = activation_test_item(&first_bytes, 1);
        storage
            .receive_sync_item(
                first.metadata.name.clone(),
                first.metadata.path.clone(),
                first.metadata.revision,
                first.metadata.description.clone(),
                &first.manifest,
                &first.chunks,
                &[],
                vec![b"abcd".to_vec(), b"efgh".to_vec()],
            )
            .unwrap();
        let old_handle = storage
            .open_active_revision(Path::new("artifact.bin"))
            .unwrap()
            .unwrap();

        let second_bytes = b"ijklmnop".to_vec();
        let second = activation_test_item(&second_bytes, 2);
        storage
            .receive_sync_item(
                second.metadata.name.clone(),
                second.metadata.path.clone(),
                second.metadata.revision,
                second.metadata.description.clone(),
                &second.manifest,
                &second.chunks,
                &[],
                vec![b"ijkl".to_vec(), b"mnop".to_vec()],
            )
            .unwrap();
        let current_handle = storage
            .open_active_revision(Path::new("artifact.bin"))
            .unwrap()
            .unwrap();
        let mut old = Vec::new();
        old_handle
            .open_file()
            .unwrap()
            .read_to_end(&mut old)
            .unwrap();
        let mut current = Vec::new();
        current_handle
            .open_file()
            .unwrap()
            .read_to_end(&mut current)
            .unwrap();
        assert_eq!(old, first_bytes);
        assert_eq!(current, second_bytes);
        assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), second_bytes);
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        let restarted_handle = restarted
            .open_active_revision(Path::new("artifact.bin"))
            .unwrap()
            .unwrap();
        let mut restarted_bytes = Vec::new();
        restarted_handle
            .open_file()
            .unwrap()
            .read_to_end(&mut restarted_bytes)
            .unwrap();
        assert_eq!(restarted_bytes, b"ijklmnop");
    }

    #[test]
    fn open_active_revision_lazily_adopts_direct_path() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        storage
            .create_item(
                "legacy".to_string(),
                PathBuf::from("legacy.bin"),
                0,
                None,
                bytes::Bytes::from_static(b"legacy direct bytes"),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        assert!(storage.active_refs.read().unwrap().is_empty());

        let handle = storage
            .open_active_revision(Path::new("legacy.bin"))
            .unwrap()
            .unwrap();
        let mut bytes = Vec::new();
        handle.open_file().unwrap().read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"legacy direct bytes");
        assert_eq!(fs::read(root.join("legacy.bin")).unwrap(), bytes);
        assert_eq!(storage.active_refs.read().unwrap().len(), 1);
    }

    #[test]
    fn selector_repairs_stale_direct_path_checkpoint_before_backing_validation() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        storage
            .create_item(
                "artifact".to_string(),
                PathBuf::from("artifact.bin"),
                1,
                Some("activation fixture".to_string()),
                bytes::Bytes::from_static(b"version1"),
                ChunkAlgorithm::fixed_aligned(4, 1),
            )
            .unwrap();
        storage
            .open_active_revision(Path::new("artifact.bin"))
            .unwrap()
            .unwrap();

        let replacement = activation_test_item(b"version2", 2);
        storage
            .activate_verified_item(
                replacement.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"version2", &replacement),
            )
            .unwrap();

        // Model abrupt process loss: the direct-path checkpoint still describes
        // version 1 while the independently durable selector names version 2.
        std::mem::forget(storage);
        let restarted = FsStorage::new(root.clone()).unwrap();
        assert_eq!(
            restarted
                .get(&replacement.metadata.root.hash)
                .unwrap()
                .clone_data()
                .unwrap()
                .as_slice(),
            b"version2"
        );
        drop(restarted);

        assert_eq!(
            FsStorage::new(root)
                .unwrap()
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item(),
            &replacement
        );
    }

    #[test]
    fn activation_envelope_goldens_are_stable_and_nonempty() {
        let item = activation_test_item(b"abcdefgh", 7);
        let active = FsActivationActiveV1 {
            root_identity: [1; 32],
            destination: PathBuf::from("artifact.bin"),
            destination_identity: [2; 32],
            revision_identity: [3; 32],
            item: item.clone(),
            kind: 0,
            seals: vec![FsActivationFileSealV1 {
                relative_path: None,
                size: 8,
                device: 9,
                inode: 10,
                mode: 0o100644,
                modified_seconds: 11,
                modified_nanoseconds: 12,
            }],
        };
        let transaction = FsActivationTransactionV2 {
            root_identity: [1; 32],
            transaction_id: [4; 16],
            destination: PathBuf::from("artifact.bin"),
            destination_identity: [2; 32],
            candidate_revision_identity: [3; 32],
            previous: Some(active.clone()),
            item,
            kind: 0,
            phase: FsActivationPhaseV2::Ready,
            candidate_seals: active.seals.clone(),
            mirror_staging_path: PathBuf::from(format!(
                ".artifact.bin.distd-activation-mirror-{}",
                "04".repeat(16)
            )),
            mirror_backup_path: PathBuf::from(format!(
                ".artifact.bin.distd-activation-backup-{}",
                "04".repeat(16)
            )),
        };
        let active_payload = bitcode::serialize(&active).unwrap();
        let active_envelope = crate::durable_state::encode_envelope(
            DurableStateKind::FsActivation,
            FS_ACTIVATION_ACTIVE_SCHEMA,
            &active_payload,
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap();
        let transaction_payload = bitcode::serialize(&transaction).unwrap();
        let transaction_envelope = crate::durable_state::encode_envelope(
            DurableStateKind::FsActivation,
            FS_ACTIVATION_TRANSACTION_SCHEMA,
            &transaction_payload,
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap();

        assert_eq!(
            hex_bytes(&active_envelope),
            "444953544453544104000100010000006201000000000000b9a42e852a8da4b66814bbaab51048222567025ef644c199a70503b669d4096001010101010101010101010101010101010101010101010101010101010101010c61727469666163742e62696e02020202020202020202020202020202020202020202020202020202020202020303030303030303030303030303030303030303030303030303030303030303086172746966616374086172746966616374011261637469766174696f6e206669787475726504070c61727469666163742e62696e06084f9bad48a95198b85d824e751dcda4b959dd4829e404ea24a722d990d9a3b692060004000600040005302e322e300008617274696661637406074f9bad48a95198b85d824e751dcda4b959dd4829e404ea24a722d990d9a3b69206080402040401040404010400040100020604048c239ccb986e812c80ef5d671a3a84e0716e02a5d73ba4e52e34581ab9719000d06288d1ddee8853a8824fde7308141ad77a1cf283fd81ae071157d61f972b1500010006080609060a02a481060b060c"
        );
        assert_eq!(
            hex_bytes(&transaction_envelope),
            "444953544453544104000100020000006403000000000000bc0ad25c048cb023353821ed764918ab97b5e929d84d65cbe10c1ed8fb54507b0101010101010101010101010101010101010101010101010101010101010101040404040404040404040404040404040c61727469666163742e62696e020202020202020202020202020202020202020202020202020202020202020203030303030303030303030303030303030303030303030303030303030303030101010101010101010101010101010101010101010101010101010101010101010c61727469666163742e62696e02020202020202020202020202020202020202020202020202020202020202020303030303030303030303030303030303030303030303030303030303030303086172746966616374086172746966616374011261637469766174696f6e206669787475726504070c61727469666163742e62696e06084f9bad48a95198b85d824e751dcda4b959dd4829e404ea24a722d990d9a3b692060004000600040005302e322e300008617274696661637406074f9bad48a95198b85d824e751dcda4b959dd4829e404ea24a722d990d9a3b69206080402040401040404010400040100020604048c239ccb986e812c80ef5d671a3a84e0716e02a5d73ba4e52e34581ab9719000d06288d1ddee8853a8824fde7308141ad77a1cf283fd81ae071157d61f972b1500010006080609060a02a481060b060c086172746966616374086172746966616374011261637469766174696f6e206669787475726504070c61727469666163742e62696e06084f9bad48a95198b85d824e751dcda4b959dd4829e404ea24a722d990d9a3b692060004000600040005302e322e300008617274696661637406074f9bad48a95198b85d824e751dcda4b959dd4829e404ea24a722d990d9a3b69206080402040401040404010400040100020604048c239ccb986e812c80ef5d671a3a84e0716e02a5d73ba4e52e34581ab9719000d06288d1ddee8853a8824fde7308141ad77a1cf283fd81ae071157d61f972b150001010006080609060a02a481060b060c462e61727469666163742e62696e2e64697374642d61637469766174696f6e2d6d6972726f722d3034303430343034303430343034303430343034303430343034303430343034462e61727469666163742e62696e2e64697374642d61637469766174696f6e2d6261636b75702d3034303430343034303430343034303430343034303430343034303430343034"
        );
    }

    #[test]
    fn recovery_removes_shared_temp_records_before_decoding_canonical_records() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let bytes = b"abcdefgh";
        let item = activation_test_item(bytes, 1);
        storage
            .activate_verified_item(
                item,
                Path::new("artifact.bin"),
                &activation_staged_data(bytes, &activation_test_item(bytes, 1)),
            )
            .unwrap();
        let active_directory = storage.activation_root().join("active");
        let active_path = fs::read_dir(&active_directory)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let active_name = active_path.file_name().unwrap().to_str().unwrap();
        let active_temp = active_directory.join(format!(".{active_name}.distd-tmp-123-1"));
        fs::copy(&active_path, &active_temp).unwrap();
        let transactions = storage.activation_root().join("transactions");
        let transaction_temp =
            transactions.join(format!(".{}.state.distd-tmp-123-2", "0".repeat(32)));
        fs::write(&transaction_temp, b"partial").unwrap();
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        assert!(!active_temp.exists());
        assert!(!transaction_temp.exists());
        assert!(restarted
            .open_active_revision(Path::new("artifact.bin"))
            .unwrap()
            .is_some());
    }

    #[test]
    fn activation_directory_entry_cap_counts_mixed_names_before_classification() {
        let canonical = format!("{}.state", "a".repeat(64));
        let temporary = format!(".{canonical}.distd-tmp-123-1");
        let names = (0..=FS_ACTIVATION_MAX_RECORDS)
            .map(|index| match index % 3 {
                0 => canonical.as_str(),
                1 => temporary.as_str(),
                _ => "noncanonical-record",
            })
            .collect::<Vec<_>>();
        for (index, name) in names.iter().take(FS_ACTIVATION_MAX_RECORDS).enumerate() {
            FsStorage::validate_activation_directory_entry_count(index + 1).unwrap();
            let _classified = FsStorage::is_hex_state_name(name, 64)
                || FsStorage::is_shared_durable_temp(name, 64);
        }
        assert!(FsStorage::validate_activation_directory_entry_count(names.len()).is_err());
    }

    #[test]
    fn activation_is_idempotent_but_rejects_non_monotonic_and_cross_artifact_writes() {
        let root = temp_path();
        let storage = FsStorage::new(root).unwrap();
        let bytes = b"abcdefgh";
        let item = activation_test_item(bytes, 1);
        let staged = activation_staged_data(bytes, &item);
        let first = storage
            .activate_verified_item(item.clone(), Path::new("artifact.bin"), &staged)
            .unwrap();
        let exact = storage
            .activate_verified_item(item.clone(), Path::new("artifact.bin"), &staged)
            .unwrap();
        assert_eq!(first.item(), exact.item());

        let mut same_revision = item.clone();
        same_revision.metadata.description = Some("forged same revision".to_string());
        assert!(storage
            .activate_verified_item(same_revision, Path::new("artifact.bin"), &staged)
            .is_err());

        let other =
            activation_test_item(b"ijklmnop", 2).with_artifact_id("other-artifact".to_string());
        assert!(storage
            .activate_verified_item(
                other.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"ijklmnop", &other),
            )
            .is_err());
        assert_eq!(
            storage
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item(),
            &item
        );
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_nested_mirror_component_is_rejected_before_selector_switch() {
        use std::os::unix::fs::symlink;

        let root = temp_path();
        let outside = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        fs::create_dir(root.join("artifact")).unwrap();
        symlink(&outside, root.join("artifact/nested")).unwrap();
        let bytes = b"data";
        let info = ChunkInfo {
            hash: do_hash(bytes),
            size: bytes.len() as u64,
        };
        let mut item = Item::make_with_entries(
            "artifact".to_string(),
            PathBuf::from("artifact"),
            1,
            None,
            info,
            vec![info],
            vec![FileEntry {
                relative_path: "nested/file".to_string(),
                size: info.size,
                chunk_range: (0, 1),
            }],
            ChunkAlgorithm::default(),
        )
        .unwrap();
        item.metadata.created = std::time::SystemTime::UNIX_EPOCH;
        item.metadata.updated = std::time::SystemTime::UNIX_EPOCH;
        let staged = activation_staged_data(bytes, &item);

        assert!(storage
            .activate_verified_item(item, Path::new("artifact"), &staged)
            .is_err());
        assert!(storage.active_refs.read().unwrap().is_empty());
        assert!(fs::symlink_metadata(root.join("artifact/nested"))
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[test]
    fn unchanged_hardlink_preserves_prior_seal_across_ready_transaction_restart() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let first = activation_directory_item(b"aaaa", b"bbbb", 1);
        let first_bytes = b"aaaabbbb";
        storage
            .activate_verified_item(
                first.clone(),
                Path::new("artifact-dir"),
                &activation_staged_data(first_bytes, &first),
            )
            .unwrap();
        let destination_identity = storage
            .activation_destination_identity(Path::new("artifact-dir"))
            .unwrap();
        let previous = storage
            .active_refs
            .read()
            .unwrap()
            .get(&destination_identity)
            .unwrap()
            .as_ref()
            .clone();

        let second = activation_directory_item(b"aaaa", b"cccc", 2);
        let second_bytes = b"aaaacccc";
        let revision_identity = FsStorage::activation_revision_identity(&second).unwrap();
        let transaction_id = [42; 16];
        let transaction_path = storage.activation_transaction_path(&transaction_id);
        let staging_root = storage.activation_staging_root(&transaction_id);
        fs::create_dir(staging_root.parent().unwrap()).unwrap();
        storage
            .materialize_activation_tree(
                &staging_root,
                ActiveRevisionKind::Directory,
                &second.manifest,
                &second.chunks,
                &activation_staged_data(second_bytes, &second),
                Some(&previous),
            )
            .unwrap();
        let revision_root = storage.activation_revision_root(&revision_identity);
        storage
            .ensure_activation_digest_directory(&revision_root)
            .unwrap();
        fs::rename(&staging_root, &revision_root).unwrap();
        FsStorage::sync_parent(revision_root.parent().unwrap()).unwrap();
        let seals = FsStorage::collect_activation_seals(
            &revision_root,
            &second,
            ActiveRevisionKind::Directory,
        )
        .unwrap();
        let transaction = FsActivationTransactionV2 {
            root_identity: fs_storage_root_identity(&root).unwrap(),
            transaction_id,
            destination: PathBuf::from("artifact-dir"),
            destination_identity,
            candidate_revision_identity: revision_identity,
            previous: Some(previous.clone()),
            item: second,
            kind: FsStorage::encode_activation_kind(ActiveRevisionKind::Directory),
            phase: FsActivationPhaseV2::Ready,
            candidate_seals: seals,
            mirror_staging_path: storage
                .activation_mirror_paths(Path::new("artifact-dir"), &transaction_id)
                .unwrap()
                .0,
            mirror_backup_path: storage
                .activation_mirror_paths(Path::new("artifact-dir"), &transaction_id)
                .unwrap()
                .1,
        };
        storage
            .write_activation_record(
                &transaction_path,
                FS_ACTIVATION_TRANSACTION_SCHEMA,
                ActivationRecordKind::Transaction(ActivationTransactionPhase::Ready),
                &transaction,
            )
            .unwrap();
        storage
            .validate_active_ref(&storage.active_ref_path(&destination_identity), &previous)
            .unwrap();
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        assert_eq!(
            restarted
                .open_active_revision(Path::new("artifact-dir"))
                .unwrap()
                .unwrap()
                .item(),
            &first
        );
        assert!(!transaction_path.exists());
    }

    #[test]
    fn recovery_completes_file_to_directory_mirror_after_old_destination_rename() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let old = transition_file_item(b"old!", 1);
        storage
            .activate_verified_item(
                old.clone(),
                Path::new("target"),
                &activation_staged_data(b"old!", &old),
            )
            .unwrap();
        let identity = storage
            .activation_destination_identity(Path::new("target"))
            .unwrap();
        let previous = storage.active_refs.read().unwrap()[&identity]
            .as_ref()
            .clone();
        let new = transition_directory_item(b"aaaa", b"bbbb", 2);
        storage
            .activate_verified_item(
                new.clone(),
                Path::new("target"),
                &activation_staged_data(b"aaaabbbb", &new),
            )
            .unwrap();
        let active = storage.active_refs.read().unwrap()[&identity]
            .as_ref()
            .clone();
        let transaction = install_switched_transaction(&storage, previous, &active, [51; 16]);
        let destination = root.join("target");
        FsStorage::remove_activation_path_checked(&destination).unwrap();
        let backup = storage.path(&transaction.mirror_backup_path).unwrap();
        fs::write(&backup, b"old!").unwrap();
        File::open(backup.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
        drop(storage);

        for _ in 0..2 {
            let restarted = FsStorage::new(root.clone()).unwrap();
            assert_eq!(fs::read(root.join("target/a.bin")).unwrap(), b"aaaa");
            assert_eq!(fs::read(root.join("target/b.bin")).unwrap(), b"bbbb");
            assert!(!restarted
                .path(&transaction.mirror_staging_path)
                .unwrap()
                .exists());
            assert!(!restarted
                .path(&transaction.mirror_backup_path)
                .unwrap()
                .exists());
            drop(restarted);
        }
    }

    #[test]
    fn recovery_completes_directory_to_file_mirror_after_old_destination_rename() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let old = transition_directory_item(b"aaaa", b"bbbb", 1);
        storage
            .activate_verified_item(
                old.clone(),
                Path::new("target"),
                &activation_staged_data(b"aaaabbbb", &old),
            )
            .unwrap();
        let identity = storage
            .activation_destination_identity(Path::new("target"))
            .unwrap();
        let previous = storage.active_refs.read().unwrap()[&identity]
            .as_ref()
            .clone();
        let new = transition_file_item(b"new!", 2);
        storage
            .activate_verified_item(
                new.clone(),
                Path::new("target"),
                &activation_staged_data(b"new!", &new),
            )
            .unwrap();
        let active = storage.active_refs.read().unwrap()[&identity]
            .as_ref()
            .clone();
        let transaction = install_switched_transaction(&storage, previous, &active, [52; 16]);
        let destination = root.join("target");
        FsStorage::remove_activation_path_checked(&destination).unwrap();
        let backup = storage.path(&transaction.mirror_backup_path).unwrap();
        fs::create_dir(&backup).unwrap();
        fs::write(backup.join("a.bin"), b"aaaa").unwrap();
        fs::write(backup.join("b.bin"), b"bbbb").unwrap();
        FsStorage::sync_directory_tree(&backup).unwrap();
        drop(storage);

        for _ in 0..2 {
            let restarted = FsStorage::new(root.clone()).unwrap();
            assert_eq!(fs::read(root.join("target")).unwrap(), b"new!");
            assert!(!restarted
                .path(&transaction.mirror_staging_path)
                .unwrap()
                .exists());
            assert!(!restarted
                .path(&transaction.mirror_backup_path)
                .unwrap()
                .exists());
            drop(restarted);
        }
    }

    #[test]
    fn ordinary_item_sharing_selected_chunk_still_migrates_to_cas() {
        let root = temp_path();
        let storage = FsStorage::new(root).unwrap();
        let bytes = b"shared immutable bytes";
        let active = activation_test_item(bytes, 1);
        storage
            .activate_verified_item(
                active.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(bytes, &active),
            )
            .unwrap();
        storage
            .create_item(
                "ordinary".to_string(),
                PathBuf::from("ordinary.bin"),
                1,
                None,
                bytes::Bytes::copy_from_slice(bytes),
                active.manifest.chunk_algorithm,
            )
            .unwrap();

        assert!(storage.migrate_referenced_chunks_to_cas().unwrap());
        for chunk in &active.chunks {
            let path = storage.revision_chunk_path(&chunk.hash).unwrap();
            assert!(path.is_file());
            FsStorage::verify_cas_chunk(&path, chunk).unwrap();
        }
    }

    #[test]
    fn immutable_reader_cache_is_globally_bounded() {
        let root = temp_path();
        let storage = FsStorage::new(root).unwrap();
        for index in 0..(DEFAULT_IMMUTABLE_READER_ENTRIES + 17) {
            let path = storage
                .activation_root()
                .join("reader-fixtures")
                .join(format!("{index}.bin"));
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, [index as u8]).unwrap();
            let mut byte = [0];
            storage.read_immutable_backing(&path, 0, &mut byte).unwrap();
            assert_eq!(byte, [index as u8]);
        }
        assert!(
            storage.immutable_readers.lock().unwrap().entries.len()
                <= DEFAULT_IMMUTABLE_READER_ENTRIES
        );
    }

    #[test]
    fn preexisting_mirror_transaction_paths_fail_before_intent_and_survive_restarts() {
        for use_backup in [false, true] {
            for collision_is_directory in [false, true] {
                let root = temp_path();
                let storage = FsStorage::new(root.clone()).unwrap();
                let old = activation_test_item(b"old-old!", 1);
                storage
                    .activate_verified_item(
                        old.clone(),
                        Path::new("artifact.bin"),
                        &activation_staged_data(b"old-old!", &old),
                    )
                    .unwrap();

                let transaction_id = [91 + u8::from(use_backup); 16];
                storage.set_next_activation_transaction_id(transaction_id);
                let (staging, backup) = storage
                    .activation_mirror_paths(Path::new("artifact.bin"), &transaction_id)
                    .unwrap();
                let collision = storage
                    .path(if use_backup { &backup } else { &staging })
                    .unwrap();
                if collision_is_directory {
                    fs::create_dir(&collision).unwrap();
                    fs::write(collision.join("owned.txt"), b"preserve-directory").unwrap();
                } else {
                    fs::write(&collision, b"preserve-file").unwrap();
                }

                let new = activation_test_item(b"new-new!", 2);
                assert!(
                    storage
                        .activate_verified_item(
                            new,
                            Path::new("artifact.bin"),
                            &activation_staged_data(
                                b"new-new!",
                                &activation_test_item(b"new-new!", 2),
                            ),
                        )
                        .is_err()
                );
                assert!(!storage
                    .activation_transaction_path(&transaction_id)
                    .exists());
                drop(storage);

                for _ in 0..2 {
                    let restarted = FsStorage::new(root.clone()).unwrap();
                    assert_eq!(
                        restarted
                            .open_active_revision(Path::new("artifact.bin"))
                            .unwrap()
                            .unwrap()
                            .item(),
                        &old
                    );
                    assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), b"old-old!");
                    if collision_is_directory {
                        assert_eq!(
                            fs::read(collision.join("owned.txt")).unwrap(),
                            b"preserve-directory"
                        );
                    } else {
                        assert_eq!(fs::read(&collision).unwrap(), b"preserve-file");
                    }
                    drop(restarted);
                }
            }
        }
    }

    #[test]
    fn injected_crash_preserves_snapshot_while_ordinary_materialize_error_rolls_back() {
        let ordinary_root = temp_path();
        let ordinary = FsStorage::new(ordinary_root).unwrap();
        let ordinary_id = [101; 16];
        ordinary.set_next_activation_transaction_id(ordinary_id);
        let item = activation_test_item(b"ordinary", 1);
        assert!(ordinary
            .activate_verified_item(item, Path::new("artifact.bin"), &HashMap::default(),)
            .is_err());
        assert!(!ordinary.simulated_crash());
        assert!(!ordinary.activation_transaction_path(&ordinary_id).exists());
        assert!(!ordinary
            .activation_staging_root(&ordinary_id)
            .parent()
            .unwrap()
            .exists());

        let crash_root = temp_path();
        let crash = FsStorage::new(crash_root.clone()).unwrap();
        let crash_id = [102; 16];
        crash.set_next_activation_transaction_id(crash_id);
        crash.set_activation_fault_hook(|stage| {
            if stage == ActivationFaultStage::Operation(ActivationOperation::MaterializeFileCreate)
            {
                return Err(std::io::Error::other("simulated process crash"));
            }
            Ok(())
        });
        let item = activation_test_item(b"snapshot", 1);
        assert!(crash
            .activate_verified_item(
                item.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"snapshot", &item),
            )
            .is_err());
        assert!(crash.simulated_crash());
        assert!(crash.activation_transaction_path(&crash_id).is_file());
        assert!(crash
            .activation_staging_root(&crash_id)
            .parent()
            .unwrap()
            .is_dir());
        drop(crash);

        for _ in 0..2 {
            let restarted = FsStorage::new(crash_root.clone()).unwrap();
            assert!(restarted
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .is_none());
            assert!(!restarted.activation_transaction_path(&crash_id).exists());
            assert!(!restarted
                .activation_staging_root(&crash_id)
                .parent()
                .unwrap()
                .exists());
            drop(restarted);
        }
    }

    #[test]
    fn ordinary_preselector_errors_cleanup_and_allow_identical_same_process_retry() {
        for (index, operation) in [
            ActivationOperation::StagingParentSync,
            ActivationOperation::RevisionParentSync,
        ]
        .into_iter()
        .enumerate()
        {
            let root = temp_path();
            let storage = FsStorage::new(root.clone()).unwrap();
            let failed_transaction_id = [u8::try_from(index + 103).unwrap(); 16];
            storage.set_next_activation_transaction_id(failed_transaction_id);
            let fired = Arc::new(AtomicBool::new(false));
            let hook_fired = fired.clone();
            storage.set_activation_error_hook(move |stage| {
                if stage == ActivationFaultStage::Operation(operation)
                    && !hook_fired.swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    return Err(std::io::Error::other("ordinary pre-selector failure"));
                }
                Ok(())
            });
            let item = activation_test_item(b"retry-me", 1);
            let staged = activation_staged_data(b"retry-me", &item);
            assert!(storage
                .activate_verified_item(item.clone(), Path::new("artifact.bin"), &staged)
                .is_err());
            assert!(fired.load(std::sync::atomic::Ordering::Acquire));
            assert!(!storage.simulated_crash());
            assert!(!storage
                .activation_transaction_path(&failed_transaction_id)
                .exists());
            assert!(!storage
                .activation_staging_root(&failed_transaction_id)
                .parent()
                .unwrap()
                .exists());
            assert!(!storage
                .activation_poisoned
                .load(std::sync::atomic::Ordering::Acquire));
            assert!(storage
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .is_none());

            storage.clear_activation_fault_hook();
            let active = storage
                .activate_verified_item(item.clone(), Path::new("artifact.bin"), &staged)
                .unwrap();
            assert_eq!(active.item(), &item);
            assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), b"retry-me");
            drop(storage);

            for _ in 0..2 {
                let restarted = FsStorage::new(root.clone()).unwrap();
                assert_eq!(
                    restarted
                        .open_active_revision(Path::new("artifact.bin"))
                        .unwrap()
                        .unwrap()
                        .item(),
                    &item
                );
                assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), b"retry-me");
                drop(restarted);
            }
        }
    }

    #[test]
    fn namespace_digest_and_staging_crash_boundaries_recover_twice() {
        let operations = [
            ActivationOperation::NamespaceCreate,
            ActivationOperation::NamespaceParentSync,
            ActivationOperation::StagingCreate,
            ActivationOperation::StagingParentSync,
            ActivationOperation::RevisionDigestCreate,
            ActivationOperation::RevisionDigestParentSync,
        ];
        for operation in operations {
            let root = temp_path();
            let storage = FsStorage::new(root.clone()).unwrap();
            let transaction_id = [operation as u8 + 110; 16];
            storage.set_next_activation_transaction_id(transaction_id);
            let fired = Arc::new(AtomicBool::new(false));
            let hook_fired = fired.clone();
            storage.set_activation_fault_hook(move |stage| {
                if stage == ActivationFaultStage::Operation(operation)
                    && !hook_fired.swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    return Err(std::io::Error::other("simulated namespace crash"));
                }
                Ok(())
            });
            let item = activation_test_item(b"snapshot", 1);
            assert!(storage
                .activate_verified_item(
                    item.clone(),
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"snapshot", &item),
                )
                .is_err());
            assert!(fired.load(std::sync::atomic::Ordering::Acquire));
            assert!(storage.simulated_crash());
            drop(storage);

            for _ in 0..2 {
                let restarted = FsStorage::new(root.clone()).unwrap();
                assert!(restarted
                    .open_active_revision(Path::new("artifact.bin"))
                    .unwrap()
                    .is_none());
                drop(restarted);
            }
        }
    }

    #[test]
    fn shared_durable_temp_snapshots_for_every_transaction_phase_recover_twice() {
        #[derive(Clone, Copy)]
        enum TempSnapshot {
            Created,
            PartialWrite,
            FullWrite,
            FileSynced,
        }

        for (phase_index, phase) in [
            FsActivationPhaseV2::Intent,
            FsActivationPhaseV2::Ready,
            FsActivationPhaseV2::Switched,
        ]
        .into_iter()
        .enumerate()
        {
            for (snapshot_index, snapshot) in [
                TempSnapshot::Created,
                TempSnapshot::PartialWrite,
                TempSnapshot::FullWrite,
                TempSnapshot::FileSynced,
            ]
            .into_iter()
            .enumerate()
            {
                let root = temp_path();
                let storage = FsStorage::new(root.clone()).unwrap();
                let first = activation_test_item(b"version1", 1);
                storage
                    .activate_verified_item(
                        first.clone(),
                        Path::new("artifact.bin"),
                        &activation_staged_data(b"version1", &first),
                    )
                    .unwrap();
                let identity = storage
                    .activation_destination_identity(Path::new("artifact.bin"))
                    .unwrap();
                let previous = storage.active_refs.read().unwrap()[&identity]
                    .as_ref()
                    .clone();
                let second = activation_test_item(b"version2", 2);
                storage
                    .activate_verified_item(
                        second.clone(),
                        Path::new("artifact.bin"),
                        &activation_staged_data(b"version2", &second),
                    )
                    .unwrap();
                let active = storage.active_refs.read().unwrap()[&identity]
                    .as_ref()
                    .clone();
                let transaction_id =
                    [u8::try_from(phase_index * 3 + snapshot_index + 120).unwrap(); 16];
                let (mirror_staging_path, mirror_backup_path) = storage
                    .activation_mirror_paths(Path::new("artifact.bin"), &transaction_id)
                    .unwrap();
                let transaction = FsActivationTransactionV2 {
                    root_identity: active.root_identity,
                    transaction_id,
                    destination: active.destination.clone(),
                    destination_identity: active.destination_identity,
                    candidate_revision_identity: active.revision_identity,
                    previous: Some(previous),
                    item: active.item.clone(),
                    kind: active.kind,
                    phase,
                    candidate_seals: if phase == FsActivationPhaseV2::Intent {
                        Vec::new()
                    } else {
                        active.seals.clone()
                    },
                    mirror_staging_path,
                    mirror_backup_path,
                };
                let payload = bitcode::serialize(&transaction).unwrap();
                let encoded = crate::durable_state::encode_envelope(
                    DurableStateKind::FsActivation,
                    FS_ACTIVATION_TRANSACTION_SCHEMA,
                    &payload,
                    MAX_DURABLE_STATE_PAYLOAD,
                )
                .unwrap();
                let canonical = storage.activation_transaction_path(&transaction_id);
                let canonical_name = canonical.file_name().unwrap().to_str().unwrap();
                let temporary = canonical
                    .parent()
                    .unwrap()
                    .join(format!(".{canonical_name}.distd-tmp-999-1"));
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary)
                    .unwrap();
                let expected = match snapshot {
                    TempSnapshot::Created => Vec::new(),
                    TempSnapshot::PartialWrite => {
                        let partial = encoded[..encoded.len() / 2].to_vec();
                        file.write_all(&partial).unwrap();
                        partial
                    }
                    TempSnapshot::FullWrite => {
                        file.write_all(&encoded).unwrap();
                        encoded
                    }
                    TempSnapshot::FileSynced => {
                        file.write_all(&encoded).unwrap();
                        file.sync_all().unwrap();
                        encoded
                    }
                };
                drop(file);
                assert_eq!(fs::read(&temporary).unwrap(), expected);
                drop(storage);

                for _ in 0..2 {
                    let restarted = FsStorage::new(root.clone()).unwrap();
                    assert!(!temporary.exists());
                    assert_eq!(
                        restarted
                            .open_active_revision(Path::new("artifact.bin"))
                            .unwrap()
                            .unwrap()
                            .item(),
                        &second
                    );
                    assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), b"version2");
                    drop(restarted);
                }
            }
        }
    }

    #[test]
    fn malformed_active_and_transaction_records_fail_closed_without_losing_old_state() {
        #[derive(Clone, Copy, Debug)]
        enum RecordFixture {
            Active,
            Transaction,
        }
        #[derive(Clone, Copy, Debug)]
        enum Malformation {
            WrongKind,
            FutureSchema,
            Truncated,
            Trailing,
            Oversized,
            PathMismatch,
            KindMismatch,
            PreviousMismatch,
            PhaseMismatch,
        }

        let framing_and_semantic = [
            Malformation::WrongKind,
            Malformation::FutureSchema,
            Malformation::Truncated,
            Malformation::Trailing,
            Malformation::Oversized,
            Malformation::PathMismatch,
            Malformation::KindMismatch,
            Malformation::PreviousMismatch,
            Malformation::PhaseMismatch,
        ];
        for record_fixture in [RecordFixture::Active, RecordFixture::Transaction] {
            for malformation in framing_and_semantic {
                if matches!(record_fixture, RecordFixture::Active)
                    && matches!(
                        malformation,
                        Malformation::PreviousMismatch | Malformation::PhaseMismatch
                    )
                {
                    continue;
                }
                let root = temp_path();
                let storage = FsStorage::new(root.clone()).unwrap();
                let old = activation_test_item(b"old-state", 1);
                storage
                    .activate_verified_item(
                        old.clone(),
                        Path::new("artifact.bin"),
                        &activation_staged_data(b"old-state", &old),
                    )
                    .unwrap();
                let old_identity = storage
                    .activation_destination_identity(Path::new("artifact.bin"))
                    .unwrap();
                let old_active = storage.active_refs.read().unwrap()[&old_identity]
                    .as_ref()
                    .clone();
                let old_selector_path = storage.active_ref_path(&old_identity);
                let old_selector_bytes = fs::read(&old_selector_path).unwrap();

                let (record_path, schema, mut payload) = match record_fixture {
                    RecordFixture::Active => {
                        let mut second = activation_test_item(b"other-one", 1)
                            .with_artifact_id("other-artifact".to_string());
                        second.metadata.path = PathBuf::from("other.bin");
                        storage
                            .activate_verified_item(
                                second,
                                Path::new("other.bin"),
                                &activation_staged_data(b"other-one", &{
                                    let mut item = activation_test_item(b"other-one", 1)
                                        .with_artifact_id("other-artifact".to_string());
                                    item.metadata.path = PathBuf::from("other.bin");
                                    item
                                }),
                            )
                            .unwrap();
                        let identity = storage
                            .activation_destination_identity(Path::new("other.bin"))
                            .unwrap();
                        let mut active = storage.active_refs.read().unwrap()[&identity]
                            .as_ref()
                            .clone();
                        match malformation {
                            Malformation::PathMismatch => {
                                active.item.metadata.path = PathBuf::from("wrong.bin");
                            }
                            Malformation::KindMismatch => active.kind = 1,
                            _ => {}
                        }
                        (
                            storage.active_ref_path(&identity),
                            FS_ACTIVATION_ACTIVE_SCHEMA,
                            bitcode::serialize(&active).unwrap(),
                        )
                    }
                    RecordFixture::Transaction => {
                        let transaction_id = [210
                            + match malformation {
                                Malformation::WrongKind => 0,
                                Malformation::FutureSchema => 1,
                                Malformation::Truncated => 2,
                                Malformation::Trailing => 3,
                                Malformation::Oversized => 4,
                                Malformation::PathMismatch => 5,
                                Malformation::KindMismatch => 6,
                                Malformation::PreviousMismatch => 7,
                                Malformation::PhaseMismatch => 8,
                            }; 16];
                        let new = activation_test_item(b"new-state", 2);
                        let (mirror_staging_path, mirror_backup_path) = storage
                            .activation_mirror_paths(Path::new("artifact.bin"), &transaction_id)
                            .unwrap();
                        let mut transaction =
                            FsActivationTransactionV2 {
                                root_identity: old_active.root_identity,
                                transaction_id,
                                destination: PathBuf::from("artifact.bin"),
                                destination_identity: old_identity,
                                candidate_revision_identity:
                                    FsStorage::activation_revision_identity(&new).unwrap(),
                                previous: Some(old_active.clone()),
                                item: new,
                                kind: FsStorage::encode_activation_kind(ActiveRevisionKind::File),
                                phase: FsActivationPhaseV2::Intent,
                                candidate_seals: Vec::new(),
                                mirror_staging_path,
                                mirror_backup_path,
                            };
                        match malformation {
                            Malformation::PathMismatch => {
                                transaction.item.metadata.path = PathBuf::from("wrong.bin");
                            }
                            Malformation::KindMismatch => transaction.kind = 1,
                            Malformation::PreviousMismatch => {
                                transaction.previous.as_mut().unwrap().destination =
                                    PathBuf::from("wrong.bin");
                            }
                            Malformation::PhaseMismatch => {
                                transaction.item = old.clone();
                                transaction.candidate_revision_identity =
                                    old_active.revision_identity;
                                transaction.previous = None;
                                transaction.phase = FsActivationPhaseV2::Switched;
                            }
                            _ => {}
                        }
                        (
                            storage.activation_transaction_path(&transaction_id),
                            FS_ACTIVATION_TRANSACTION_SCHEMA,
                            bitcode::serialize(&transaction).unwrap(),
                        )
                    }
                };
                let kind = if matches!(malformation, Malformation::WrongKind) {
                    DurableStateKind::FsStorage
                } else {
                    DurableStateKind::FsActivation
                };
                let encoded_schema = if matches!(malformation, Malformation::FutureSchema) {
                    schema + 100
                } else {
                    schema
                };
                let mut encoded = crate::durable_state::encode_envelope(
                    kind,
                    encoded_schema,
                    &payload,
                    MAX_DURABLE_STATE_PAYLOAD,
                )
                .unwrap();
                match malformation {
                    Malformation::Truncated => encoded.truncate(encoded.len() / 2),
                    Malformation::Trailing => encoded.push(0xa5),
                    Malformation::Oversized => {
                        encoded[16..24]
                            .copy_from_slice(&(MAX_DURABLE_STATE_PAYLOAD + 1).to_le_bytes());
                    }
                    _ => {}
                }
                payload.clear();
                fs::write(&record_path, &encoded).unwrap();
                let corrupt_bytes = fs::read(&record_path).unwrap();
                drop(storage);

                for _ in 0..2 {
                    assert!(
                        FsStorage::new(root.clone()).is_err(),
                        "{record_fixture:?} {malformation:?}"
                    );
                    assert_eq!(fs::read(&record_path).unwrap(), corrupt_bytes);
                    assert_eq!(fs::read(&old_selector_path).unwrap(), old_selector_bytes);
                    assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), b"old-state");
                }
            }
        }
    }

    #[test]
    fn recovery_mutation_faults_leave_restartable_snapshots_and_converge_twice() {
        let operations = [
            ActivationOperation::RecoveryMirrorCleanup,
            ActivationOperation::RecoveryMirrorCleanupParentSync,
            ActivationOperation::RecoveryStagingCleanup,
            ActivationOperation::RecoveryStagingCleanupParentSync,
            ActivationOperation::RecoveryTransactionCleanup,
            ActivationOperation::RecoveryTransactionCleanupParentSync,
        ];
        for (index, operation) in operations.into_iter().enumerate() {
            let root = temp_path();
            let storage = FsStorage::new(root.clone()).unwrap();
            let old = activation_test_item(b"version1", 1);
            storage
                .activate_verified_item(
                    old,
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"version1", &activation_test_item(b"version1", 1)),
                )
                .unwrap();
            let identity = storage
                .activation_destination_identity(Path::new("artifact.bin"))
                .unwrap();
            let previous = storage.active_refs.read().unwrap()[&identity]
                .as_ref()
                .clone();
            let new = activation_test_item(b"version2", 2);
            storage
                .activate_verified_item(
                    new.clone(),
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"version2", &new),
                )
                .unwrap();
            let active = storage.active_refs.read().unwrap()[&identity]
                .as_ref()
                .clone();
            let transaction_id = [u8::try_from(index + 230).unwrap(); 16];
            let transaction =
                install_switched_transaction(&storage, previous, &active, transaction_id);
            if matches!(
                operation,
                ActivationOperation::RecoveryMirrorCleanup
                    | ActivationOperation::RecoveryMirrorCleanupParentSync
            ) {
                fs::write(
                    storage.path(&transaction.mirror_staging_path).unwrap(),
                    b"owned mirror staging",
                )
                .unwrap();
            }
            if matches!(
                operation,
                ActivationOperation::RecoveryStagingCleanup
                    | ActivationOperation::RecoveryStagingCleanupParentSync
            ) {
                let staging_parent = storage
                    .activation_staging_root(&transaction_id)
                    .parent()
                    .unwrap()
                    .to_path_buf();
                fs::create_dir(&staging_parent).unwrap();
                fs::write(staging_parent.join("owned"), b"staging").unwrap();
            }
            let fired = Arc::new(AtomicBool::new(false));
            let hook_fired = fired.clone();
            storage.set_activation_fault_hook(move |stage| {
                if stage == ActivationFaultStage::Operation(operation)
                    && !hook_fired.swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    return Err(std::io::Error::other("simulated recovery crash"));
                }
                Ok(())
            });
            assert!(storage.recover_activations().is_err());
            assert!(fired.load(std::sync::atomic::Ordering::Acquire));
            drop(storage);

            for _ in 0..2 {
                let restarted = FsStorage::new(root.clone()).unwrap();
                assert_eq!(
                    restarted
                        .open_active_revision(Path::new("artifact.bin"))
                        .unwrap()
                        .unwrap()
                        .item(),
                    &new
                );
                assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), b"version2");
                assert!(!restarted
                    .activation_transaction_path(&transaction_id)
                    .exists());
                drop(restarted);
            }
        }
    }

    #[test]
    fn recovery_mirror_operation_faults_converge_twice() {
        for (index, operation) in [
            ActivationOperation::MirrorEntryCreate,
            ActivationOperation::MirrorEntryWrite,
            ActivationOperation::MirrorEntrySync,
            ActivationOperation::MirrorEntryRename,
            ActivationOperation::MirrorEntryRemove,
            ActivationOperation::MirrorDirectorySync,
        ]
        .into_iter()
        .enumerate()
        {
            let root = temp_path();
            let storage = FsStorage::new(root.clone()).unwrap();
            let old = activation_test_item(b"version1", 1);
            storage
                .activate_verified_item(
                    old,
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"version1", &activation_test_item(b"version1", 1)),
                )
                .unwrap();
            let identity = storage
                .activation_destination_identity(Path::new("artifact.bin"))
                .unwrap();
            let previous = storage.active_refs.read().unwrap()[&identity]
                .as_ref()
                .clone();
            let new = activation_test_item(b"version2", 2);
            storage
                .activate_verified_item(
                    new.clone(),
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"version2", &new),
                )
                .unwrap();
            let active = storage.active_refs.read().unwrap()[&identity]
                .as_ref()
                .clone();
            let transaction_id = [u8::try_from(index + 240).unwrap(); 16];
            install_switched_transaction(&storage, previous, &active, transaction_id);
            fs::write(root.join("artifact.bin"), b"version1").unwrap();
            let fired = Arc::new(AtomicBool::new(false));
            let hook_fired = fired.clone();
            storage.set_activation_fault_hook(move |stage| {
                if stage == ActivationFaultStage::Operation(operation)
                    && !hook_fired.swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    return Err(std::io::Error::other("simulated recovery mirror crash"));
                }
                Ok(())
            });
            assert!(storage.recover_activations().is_err());
            assert!(fired.load(std::sync::atomic::Ordering::Acquire));
            drop(storage);

            for _ in 0..2 {
                let restarted = FsStorage::new(root.clone()).unwrap();
                assert_eq!(
                    restarted
                        .open_active_revision(Path::new("artifact.bin"))
                        .unwrap()
                        .unwrap()
                        .item(),
                    &new
                );
                assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), b"version2");
                assert!(!restarted
                    .activation_transaction_path(&transaction_id)
                    .exists());
                drop(restarted);
            }
        }
    }

    #[test]
    fn recovery_record_temp_and_quarantine_faults_converge_without_byte_loss() {
        for operation in [
            ActivationOperation::RecoveryRecordTempCleanup,
            ActivationOperation::RecoveryRecordTempCleanupParentSync,
            ActivationOperation::RecoveryRecordQuarantine,
            ActivationOperation::RecoveryRecordSourceParentSync,
            ActivationOperation::RecoveryRecordQuarantineParentSync,
        ] {
            let root = temp_path();
            let storage = FsStorage::new(root.clone()).unwrap();
            let old = activation_test_item(b"old-state", 1);
            storage
                .activate_verified_item(
                    old.clone(),
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"old-state", &old),
                )
                .unwrap();
            let transactions = storage.activation_root().join("transactions");
            let source = if matches!(
                operation,
                ActivationOperation::RecoveryRecordTempCleanup
                    | ActivationOperation::RecoveryRecordTempCleanupParentSync
            ) {
                transactions.join(format!(".{}.state.distd-tmp-999-9", "a".repeat(32)))
            } else {
                transactions.join("noncanonical-record")
            };
            let original = b"exact recovery record bytes";
            fs::write(&source, original).unwrap();
            let fired = Arc::new(AtomicBool::new(false));
            let hook_fired = fired.clone();
            storage.set_activation_fault_hook(move |stage| {
                if stage == ActivationFaultStage::Operation(operation)
                    && !hook_fired.swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    return Err(std::io::Error::other("simulated record recovery crash"));
                }
                Ok(())
            });
            assert!(storage.recover_activations().is_err());
            assert!(fired.load(std::sync::atomic::Ordering::Acquire));
            if matches!(
                operation,
                ActivationOperation::RecoveryRecordTempCleanup
                    | ActivationOperation::RecoveryRecordTempCleanupParentSync
            ) {
                assert!(!source.exists());
            } else {
                let quarantine = storage.activation_root().join("quarantine");
                let quarantined = fs::read_dir(&quarantine)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.file_name()
                            .unwrap()
                            .to_string_lossy()
                            .starts_with("transaction-noncanonical-record-")
                    })
                    .unwrap();
                assert_eq!(fs::read(quarantined).unwrap(), original);
            }
            drop(storage);

            for _ in 0..2 {
                let restarted = FsStorage::new(root.clone()).unwrap();
                assert_eq!(
                    restarted
                        .open_active_revision(Path::new("artifact.bin"))
                        .unwrap()
                        .unwrap()
                        .item(),
                    &old
                );
                assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), b"old-state");
                drop(restarted);
            }
        }
    }

    #[test]
    fn named_activation_faults_recover_idempotently_through_real_file_path() {
        let durable_stages = [
            DurableReplaceStage::AfterEncode,
            DurableReplaceStage::AfterCreate,
            DurableReplaceStage::AfterWrite,
            DurableReplaceStage::AfterFileSync,
            DurableReplaceStage::AfterRename,
            DurableReplaceStage::AfterDirectorySync,
        ];
        let mut stages = vec![
            ActivationFaultStage::BeforeIntent,
            ActivationFaultStage::AfterIntent,
            ActivationFaultStage::AfterMaterialize,
            ActivationFaultStage::AfterRevisionPublish,
            ActivationFaultStage::AfterReady,
            ActivationFaultStage::AfterSelector,
            ActivationFaultStage::AfterSwitched,
            ActivationFaultStage::AfterStatePersist,
            ActivationFaultStage::AfterMirror,
        ];
        stages.extend(durable_stages.into_iter().map(|stage| {
            ActivationFaultStage::DurableRecord {
                record: ActivationRecordKind::Active,
                stage,
            }
        }));
        for phase in [ActivationTransactionPhase::Intent] {
            stages.extend(durable_stages.into_iter().map(|stage| {
                ActivationFaultStage::DurableRecord {
                    record: ActivationRecordKind::Transaction(phase),
                    stage,
                }
            }));
        }
        stages.extend(
            [
                ActivationOperation::MaterializeFileCreate,
                ActivationOperation::MaterializeFileWrite,
                ActivationOperation::MaterializeFileSync,
                ActivationOperation::RevisionRename,
                ActivationOperation::RevisionParentSync,
                ActivationOperation::MirrorEntryCreate,
                ActivationOperation::MirrorEntryWrite,
                ActivationOperation::MirrorEntrySync,
                ActivationOperation::MirrorEntryRename,
                ActivationOperation::MirrorEntryRemove,
                ActivationOperation::MirrorDirectorySync,
            ]
            .into_iter()
            .map(ActivationFaultStage::Operation),
        );
        for fault in stages {
            let root = temp_path();
            let storage = FsStorage::new(root.clone()).unwrap();
            let old = activation_test_item(b"old-old!", 1);
            storage
                .activate_verified_item(
                    old.clone(),
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"old-old!", &old),
                )
                .unwrap();
            let fired = Arc::new(AtomicBool::new(false));
            let fired_by_hook = fired.clone();
            storage.set_activation_fault_hook(move |stage| {
                if stage == fault && !fired_by_hook.swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    return Err(std::io::Error::other("injected activation fault"));
                }
                Ok(())
            });
            let new = activation_test_item(b"new-new!", 2);
            assert!(storage
                .activate_verified_item(
                    new.clone(),
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"new-new!", &new),
                )
                .is_err());
            assert!(fired.load(std::sync::atomic::Ordering::Acquire));
            drop(storage);

            let restarted = FsStorage::new(root.clone()).unwrap();
            let selected = restarted
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item()
                .clone();
            let expected = if selected.metadata.revision == 1 {
                b"old-old!".as_slice()
            } else {
                assert_eq!(selected.metadata.revision, 2);
                b"new-new!".as_slice()
            };
            assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), expected);
            drop(restarted);
            let restarted_twice = FsStorage::new(root).unwrap();
            assert_eq!(
                restarted_twice
                    .open_active_revision(Path::new("artifact.bin"))
                    .unwrap()
                    .unwrap()
                    .item()
                    .metadata
                    .revision,
                selected.metadata.revision
            );
        }
    }

    #[test]
    fn directory_operation_faults_cover_add_change_remove_reuse_and_restart_twice() {
        let old_entries: &[(&str, &[u8])] = &[
            ("change.bin", b"old!"),
            ("nested/old.bin", b"nest"),
            ("remove.bin", b"gone"),
            ("reuse.bin", b"same"),
        ];
        let new_entries: &[(&str, &[u8])] = &[
            ("add.bin", b"add!"),
            ("change.bin", b"new!"),
            ("nested/new.bin", b"next"),
            ("reuse.bin", b"same"),
        ];
        let operations = [
            ActivationOperation::MaterializeDirectoryCreate,
            ActivationOperation::MaterializeFileCreate,
            ActivationOperation::MaterializeFileWrite,
            ActivationOperation::MaterializeFileSync,
            ActivationOperation::MaterializeDirectorySync,
            ActivationOperation::RevisionRename,
            ActivationOperation::RevisionParentSync,
            ActivationOperation::MirrorDirectoryCreate,
            ActivationOperation::MirrorEntryCreate,
            ActivationOperation::MirrorEntryWrite,
            ActivationOperation::MirrorEntrySync,
            ActivationOperation::MirrorEntryRename,
            ActivationOperation::MirrorEntryRemove,
            ActivationOperation::MirrorDirectorySync,
        ];
        for operation in operations {
            let root = temp_path();
            let storage = FsStorage::new(root.clone()).unwrap();
            let (old, old_bytes) = activation_directory_fixture(old_entries, 1);
            storage
                .activate_verified_item(
                    old.clone(),
                    Path::new("artifact-dir"),
                    &activation_staged_data(&old_bytes, &old),
                )
                .unwrap();
            let fired = Arc::new(AtomicBool::new(false));
            let hook_fired = fired.clone();
            storage.set_activation_fault_hook(move |stage| {
                if stage == ActivationFaultStage::Operation(operation)
                    && !hook_fired.swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    return Err(std::io::Error::other("injected directory operation fault"));
                }
                Ok(())
            });
            let (new, new_bytes) = activation_directory_fixture(new_entries, 2);
            assert!(storage
                .activate_verified_item(
                    new.clone(),
                    Path::new("artifact-dir"),
                    &activation_staged_data(&new_bytes, &new),
                )
                .is_err());
            assert!(fired.load(std::sync::atomic::Ordering::Acquire));
            drop(storage);

            let restarted = FsStorage::new(root.clone()).unwrap();
            let selected = restarted
                .open_active_revision(Path::new("artifact-dir"))
                .unwrap()
                .unwrap()
                .item()
                .clone();
            if selected.metadata.revision == 1 {
                assert_directory_fixture(&root, old_entries);
            } else {
                assert_eq!(selected.metadata.revision, 2);
                assert_directory_fixture(&root, new_entries);
                assert!(!root.join("artifact-dir/remove.bin").exists());
                assert!(!root.join("artifact-dir/nested/old.bin").exists());
            }
            drop(restarted);
            let restarted_twice = FsStorage::new(root).unwrap();
            assert_eq!(
                restarted_twice
                    .open_active_revision(Path::new("artifact-dir"))
                    .unwrap()
                    .unwrap()
                    .item()
                    .metadata
                    .revision,
                selected.metadata.revision
            );
        }
    }

    #[test]
    fn opens_are_nonblocking_after_selector_and_old_handle_stays_pinned() {
        let root = temp_path();
        let storage = Arc::new(FsStorage::new(root).unwrap());
        let old = activation_test_item(b"old-old!", 1);
        let old_handle = storage
            .activate_verified_item(
                old,
                Path::new("artifact.bin"),
                &activation_staged_data(b"old-old!", &activation_test_item(b"old-old!", 1)),
            )
            .unwrap();
        let stop_opening = Arc::new(AtomicBool::new(false));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let opener_storage = storage.clone();
        let opener_stop = stop_opening.clone();
        let opener_observed = observed.clone();
        let opener = std::thread::spawn(move || {
            while !opener_stop.load(std::sync::atomic::Ordering::Acquire) {
                let revision = opener_storage
                    .open_active_revision(Path::new("artifact.bin"))
                    .unwrap()
                    .unwrap()
                    .item()
                    .metadata
                    .revision;
                opener_observed.lock().unwrap().push(revision);
                std::thread::yield_now();
            }
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !observed.lock().unwrap().contains(&1) {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        let reached = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let hook_reached = reached.clone();
        let hook_release = release.clone();
        storage.set_activation_fault_hook(move |stage| {
            if stage == ActivationFaultStage::AfterSelector {
                let (lock, cv) = &*hook_reached;
                *lock.lock().unwrap() = true;
                cv.notify_all();
                let (lock, cv) = &*hook_release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = cv.wait(released).unwrap();
                }
            }
            Ok(())
        });
        let activating = storage.clone();
        let new = activation_test_item(b"new-new!", 2);
        let thread = std::thread::spawn(move || {
            activating.activate_verified_item(
                new.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"new-new!", &new),
            )
        });
        let (lock, cv) = &*reached;
        let mut is_reached = lock.lock().unwrap();
        while !*is_reached {
            is_reached = cv.wait(is_reached).unwrap();
        }
        drop(is_reached);

        let started = std::time::Instant::now();
        let new_handle = storage
            .open_active_revision(Path::new("artifact.bin"))
            .unwrap()
            .unwrap();
        assert!(started.elapsed() < std::time::Duration::from_millis(50));
        assert_eq!(new_handle.item().metadata.revision, 2);
        let mut old_bytes = Vec::new();
        old_handle
            .open_file()
            .unwrap()
            .read_to_end(&mut old_bytes)
            .unwrap();
        assert_eq!(old_bytes, b"old-old!");
        let mut new_bytes = Vec::new();
        new_handle
            .open_file()
            .unwrap()
            .read_to_end(&mut new_bytes)
            .unwrap();
        assert_eq!(new_bytes, b"new-new!");

        let (lock, cv) = &*release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        thread.join().unwrap().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !observed.lock().unwrap().contains(&2) {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        stop_opening.store(true, std::sync::atomic::Ordering::Release);
        opener.join().unwrap();
        let observed = observed.lock().unwrap();
        assert!(observed.iter().all(|revision| matches!(revision, 1 | 2)));
        assert!(observed.contains(&1));
        assert!(observed.contains(&2));
    }

    #[test]
    fn same_destination_reversed_v2_v3_writers_serialize_monotonically() {
        let root = temp_path();
        let storage = Arc::new(FsStorage::new(root.clone()).unwrap());
        let first = activation_test_item(b"version1", 1);
        storage
            .activate_verified_item(
                first,
                Path::new("artifact.bin"),
                &activation_staged_data(b"version1", &activation_test_item(b"version1", 1)),
            )
            .unwrap();
        let reached = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let hook_reached = reached.clone();
        let hook_release = release.clone();
        storage.set_activation_fault_hook(move |stage| {
            if stage == ActivationFaultStage::AfterIntent {
                let (lock, cv) = &*hook_reached;
                *lock.lock().unwrap() = true;
                cv.notify_all();
                let (lock, cv) = &*hook_release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = cv.wait(released).unwrap();
                }
            }
            Ok(())
        });
        let v2_storage = storage.clone();
        let v2 = std::thread::spawn(move || {
            let item = activation_test_item(b"version2", 2);
            v2_storage.activate_verified_item(
                item.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"version2", &item),
            )
        });
        let (lock, cv) = &*reached;
        let mut is_reached = lock.lock().unwrap();
        while !*is_reached {
            is_reached = cv.wait(is_reached).unwrap();
        }
        drop(is_reached);
        let v3_storage = storage.clone();
        let v3 = std::thread::spawn(move || {
            let item = activation_test_item(b"version3", 3);
            v3_storage.activate_verified_item(
                item.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"version3", &item),
            )
        });
        std::thread::sleep(std::time::Duration::from_millis(20));
        let (lock, cv) = &*release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        v2.join().unwrap().unwrap();
        v3.join().unwrap().unwrap();
        assert_eq!(
            storage
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item()
                .metadata
                .revision,
            3
        );
        drop(storage);
        assert_eq!(
            FsStorage::new(root)
                .unwrap()
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item()
                .metadata
                .revision,
            3
        );
    }

    #[test]
    fn different_destinations_activate_concurrently_and_survive_restart() {
        let root = temp_path();
        let storage = Arc::new(FsStorage::new(root.clone()).unwrap());
        let reached = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let first_hook = Arc::new(AtomicBool::new(true));
        let hook_reached = reached.clone();
        let hook_release = release.clone();
        let hook_first = first_hook.clone();
        storage.set_activation_fault_hook(move |stage| {
            if stage == ActivationFaultStage::AfterIntent
                && hook_first.swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                let (lock, cv) = &*hook_reached;
                *lock.lock().unwrap() = true;
                cv.notify_all();
                let (lock, cv) = &*hook_release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = cv.wait(released).unwrap();
                }
            }
            Ok(())
        });
        let first_storage = storage.clone();
        let first = std::thread::spawn(move || {
            let mut item =
                activation_test_item(b"firstone", 1).with_artifact_id("first-artifact".to_string());
            item.metadata.path = PathBuf::from("first.bin");
            first_storage.activate_verified_item(
                item.clone(),
                Path::new("first.bin"),
                &activation_staged_data(b"firstone", &item),
            )
        });
        let (lock, cv) = &*reached;
        let mut is_reached = lock.lock().unwrap();
        while !*is_reached {
            is_reached = cv.wait(is_reached).unwrap();
        }
        drop(is_reached);

        let second_storage = storage.clone();
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let second = std::thread::spawn(move || {
            let mut item = activation_test_item(b"second!!", 1)
                .with_artifact_id("second-artifact".to_string());
            item.metadata.path = PathBuf::from("second.bin");
            let result = second_storage.activate_verified_item(
                item.clone(),
                Path::new("second.bin"),
                &activation_staged_data(b"second!!", &item),
            );
            completed_tx.send(result.is_ok()).unwrap();
            result
        });
        assert!(completed_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap());
        let (lock, cv) = &*release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        drop(storage);

        let restarted = FsStorage::new(root).unwrap();
        assert_eq!(
            restarted
                .open_active_revision(Path::new("first.bin"))
                .unwrap()
                .unwrap()
                .item()
                .metadata
                .artifact_id,
            "first-artifact"
        );
        assert_eq!(
            restarted
                .open_active_revision(Path::new("second.bin"))
                .unwrap()
                .unwrap()
                .item()
                .metadata
                .artifact_id,
            "second-artifact"
        );
    }

    #[test]
    fn same_destination_v3_arriving_first_makes_waiting_v2_reject() {
        let root = temp_path();
        let storage = Arc::new(FsStorage::new(root.clone()).unwrap());
        let first = activation_test_item(b"version1", 1);
        storage
            .activate_verified_item(
                first,
                Path::new("artifact.bin"),
                &activation_staged_data(b"version1", &activation_test_item(b"version1", 1)),
            )
            .unwrap();
        let reached = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let hook_reached = reached.clone();
        let hook_release = release.clone();
        storage.set_activation_fault_hook(move |stage| {
            if stage == ActivationFaultStage::AfterIntent {
                let (lock, cv) = &*hook_reached;
                *lock.lock().unwrap() = true;
                cv.notify_all();
                let (lock, cv) = &*hook_release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = cv.wait(released).unwrap();
                }
            }
            Ok(())
        });
        let v3_storage = storage.clone();
        let v3 = std::thread::spawn(move || {
            let item = activation_test_item(b"version3", 3);
            v3_storage.activate_verified_item(
                item.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"version3", &item),
            )
        });
        let (lock, cv) = &*reached;
        let mut is_reached = lock.lock().unwrap();
        while !*is_reached {
            is_reached = cv.wait(is_reached).unwrap();
        }
        drop(is_reached);
        let v2_storage = storage.clone();
        let v2 = std::thread::spawn(move || {
            let item = activation_test_item(b"version2", 2);
            v2_storage.activate_verified_item(
                item.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"version2", &item),
            )
        });
        std::thread::sleep(std::time::Duration::from_millis(20));
        let (lock, cv) = &*release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        v3.join().unwrap().unwrap();
        assert!(v2.join().unwrap().is_err());
        assert_eq!(
            storage
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item()
                .metadata
                .revision,
            3
        );
        drop(storage);
        assert_eq!(
            FsStorage::new(root)
                .unwrap()
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item()
                .metadata
                .revision,
            3
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_spawn_blocking_caller_does_not_cancel_activation_coordinator() {
        for fault_stage in [
            ActivationFaultStage::AfterIntent,
            ActivationFaultStage::AfterSelector,
        ] {
            let root = temp_path();
            let storage = Arc::new(FsStorage::new(root.clone()).unwrap());
            let storage_lifetime = Arc::downgrade(&storage);
            let first = activation_test_item(b"version1", 1);
            storage
                .activate_verified_item(
                    first,
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"version1", &activation_test_item(b"version1", 1)),
                )
                .unwrap();
            let (reached_tx, reached_rx) = std::sync::mpsc::channel();
            let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
            let hook_release = release.clone();
            let sent = Arc::new(AtomicBool::new(false));
            let hook_sent = sent.clone();
            storage.set_activation_fault_hook(move |stage| {
                if stage == fault_stage
                    && !hook_sent.swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    reached_tx.send(()).unwrap();
                    let (lock, cv) = &*hook_release;
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        released = cv.wait(released).unwrap();
                    }
                }
                Ok(())
            });
            let owned_storage = storage.clone();
            let caller = tokio::task::spawn_blocking(move || {
                let item = activation_test_item(b"version2", 2);
                owned_storage.activate_verified_item(
                    item.clone(),
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"version2", &item),
                )
            });
            reached_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            caller.abort();
            let (lock, cv) = &*release;
            *lock.lock().unwrap() = true;
            cv.notify_all();

            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if storage
                    .open_active_revision(Path::new("artifact.bin"))
                    .ok()
                    .flatten()
                    .is_some_and(|active| active.item().metadata.revision == 2)
                    && fs::read(root.join("artifact.bin")).is_ok_and(|bytes| bytes == b"version2")
                {
                    break;
                }
                assert!(std::time::Instant::now() < deadline);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            drop(storage);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while storage_lifetime.upgrade().is_some() {
                assert!(std::time::Instant::now() < deadline);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert_eq!(
                FsStorage::new(root)
                    .unwrap()
                    .open_active_revision(Path::new("artifact.bin"))
                    .unwrap()
                    .unwrap()
                    .item()
                    .metadata
                    .revision,
                2
            );
        }
    }

    #[test]
    fn type_transition_rename_and_cleanup_faults_recover_restart_twice() {
        for (file_to_directory, operation) in [
            (true, ActivationOperation::MirrorEntryRename),
            (true, ActivationOperation::MirrorEntryRemove),
            (false, ActivationOperation::MirrorEntryRename),
            (false, ActivationOperation::MirrorEntryRemove),
        ] {
            let root = temp_path();
            let storage = FsStorage::new(root.clone()).unwrap();
            let (old, old_bytes, new, new_bytes) = if file_to_directory {
                (
                    transition_file_item(b"old!", 1),
                    b"old!".to_vec(),
                    transition_directory_item(b"aaaa", b"bbbb", 2),
                    b"aaaabbbb".to_vec(),
                )
            } else {
                (
                    transition_directory_item(b"aaaa", b"bbbb", 1),
                    b"aaaabbbb".to_vec(),
                    transition_file_item(b"new!", 2),
                    b"new!".to_vec(),
                )
            };
            storage
                .activate_verified_item(
                    old.clone(),
                    Path::new("target"),
                    &activation_staged_data(&old_bytes, &old),
                )
                .unwrap();
            let fired = Arc::new(AtomicBool::new(false));
            let hook_fired = fired.clone();
            storage.set_activation_fault_hook(move |stage| {
                if stage == ActivationFaultStage::Operation(operation)
                    && !hook_fired.swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    return Err(std::io::Error::other("injected type-transition fault"));
                }
                Ok(())
            });
            assert!(storage
                .activate_verified_item(
                    new.clone(),
                    Path::new("target"),
                    &activation_staged_data(&new_bytes, &new),
                )
                .is_err());
            assert!(fired.load(std::sync::atomic::Ordering::Acquire));
            drop(storage);

            for _ in 0..2 {
                let restarted = FsStorage::new(root.clone()).unwrap();
                assert_eq!(
                    restarted
                        .open_active_revision(Path::new("target"))
                        .unwrap()
                        .unwrap()
                        .item(),
                    &new
                );
                if file_to_directory {
                    assert_eq!(fs::read(root.join("target/a.bin")).unwrap(), b"aaaa");
                    assert_eq!(fs::read(root.join("target/b.bin")).unwrap(), b"bbbb");
                } else {
                    assert_eq!(fs::read(root.join("target")).unwrap(), b"new!");
                }
                drop(restarted);
            }
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn incremental_file_activation_clones_prior_revision_and_writes_only_changed_chunks() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let old = activation_test_item(b"aaaabbbbcccc", 1);
        storage
            .activate_verified_item(
                old.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"aaaabbbbcccc", &old),
            )
            .unwrap();

        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = writes.clone();
        storage.set_activation_fault_hook(move |stage| {
            if stage == ActivationFaultStage::Operation(ActivationOperation::MaterializeFileWrite) {
                observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(())
        });
        let new = activation_test_item(b"aaaaxxxxcccc", 2);
        storage
            .activate_verified_item(
                new.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"aaaaxxxxcccc", &new),
            )
            .unwrap();

        assert_eq!(writes.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(
            fs::read(root.join("artifact.bin")).unwrap(),
            b"aaaaxxxxcccc"
        );
    }

    #[test]
    fn retained_ready_allows_next_activation_and_restart_rebuilds_checkpoint_and_links() {
        let root = temp_path();
        {
            let storage = FsStorage::new(root.clone()).unwrap();
            storage
                .create_item(
                    "artifact".to_string(),
                    PathBuf::from("artifact.bin"),
                    1,
                    Some("activation fixture".to_string()),
                    bytes::Bytes::from_static(b"aaaabbbbcccc"),
                    ChunkAlgorithm::fixed_aligned(4, 1),
                )
                .unwrap();
        }

        let storage = FsStorage::new(root.clone()).unwrap();
        assert_eq!(
            storage
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item()
                .metadata
                .revision,
            1
        );
        let checkpoint_path = storage.inner.read().unwrap().persistance_path.clone();
        let checkpoint_before = fs::read(&checkpoint_path).unwrap();
        let second = activation_test_item(b"aaaaxxxxcccc", 2);
        storage
            .activate_verified_item(
                second.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"aaaaxxxxcccc", &second),
            )
            .unwrap();
        assert_eq!(fs::read(&checkpoint_path).unwrap(), checkpoint_before);

        let transaction_directory = storage.activation_root().join("transactions");
        let third = activation_test_item(b"yyyyxxxxcccc", 3);
        storage
            .activate_verified_item(
                third.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"yyyyxxxxcccc", &third),
            )
            .unwrap();
        assert_eq!(fs::read(&checkpoint_path).unwrap(), checkpoint_before);

        // Model process loss: do not allow Drop to flush the derived checkpoint.
        std::mem::forget(storage);
        let restarted = FsStorage::new(root.clone()).unwrap();
        let handle = restarted
            .open_active_revision(Path::new("artifact.bin"))
            .unwrap()
            .unwrap();
        assert_eq!(handle.item(), &third);
        assert_eq!(
            restarted
                .get(&third.metadata.root.hash)
                .unwrap()
                .clone_data()
                .unwrap()
                .as_slice(),
            b"yyyyxxxxcccc"
        );
        assert!(restarted
            .inner
            .read()
            .unwrap()
            .links
            .contains_key(&third.metadata.root.hash));
        assert_ne!(fs::read(&checkpoint_path).unwrap(), checkpoint_before);
        assert_eq!(
            fs::read_dir(&transaction_directory)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry
                    .path()
                    .extension()
                    .is_some_and(|value| value == "state"))
                .count(),
            0
        );
        drop(restarted);

        let restarted_twice = FsStorage::new(root).unwrap();
        assert_eq!(
            restarted_twice
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item(),
            &third
        );
    }

    #[test]
    fn deferred_intent_cleanup_fault_poison_rejects_next_activation_until_restart() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        storage.set_activation_error_hook(|stage| {
            if stage
                == ActivationFaultStage::Operation(ActivationOperation::RecoveryTransactionCleanup)
            {
                Err(std::io::Error::other("deferred cleanup fault"))
            } else {
                Ok(())
            }
        });
        let first = activation_test_item(b"version1", 1);
        storage
            .activate_verified_item(
                first.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"version1", &first),
            )
            .unwrap();
        for _ in 0..100 {
            if storage
                .activation_poisoned
                .load(std::sync::atomic::Ordering::Acquire)
            {
                break;
            }
            std::thread::yield_now();
        }
        assert!(storage
            .activation_poisoned
            .load(std::sync::atomic::Ordering::Acquire));
        storage.clear_activation_fault_hook();
        let second = activation_test_item(b"version2", 2);
        assert!(storage
            .activate_verified_item(
                second.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"version2", &second),
            )
            .is_err());
        drop(storage);

        let restarted = FsStorage::new(root.clone()).unwrap();
        assert_eq!(
            restarted
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item(),
            &first
        );
        restarted
            .activate_verified_item(
                second.clone(),
                Path::new("artifact.bin"),
                &activation_staged_data(b"version2", &second),
            )
            .unwrap();
        drop(restarted);
        assert_eq!(
            FsStorage::new(root)
                .unwrap()
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item(),
            &second
        );
    }

    #[test]
    fn valid_selector_does_not_suppress_unrelated_corrupt_checkpoint_backing() {
        let root = temp_path();
        {
            let storage = FsStorage::new(root.clone()).unwrap();
            storage
                .create_item(
                    "ordinary".to_string(),
                    PathBuf::from("ordinary.bin"),
                    1,
                    None,
                    bytes::Bytes::from_static(b"ordinary"),
                    ChunkAlgorithm::fixed_aligned(4, 1),
                )
                .unwrap();
            let active = activation_test_item(b"selected", 1);
            storage
                .activate_verified_item(
                    active.clone(),
                    Path::new("artifact.bin"),
                    &activation_staged_data(b"selected", &active),
                )
                .unwrap();
        }
        fs::write(root.join("ordinary.bin"), b"corrupt!").unwrap();
        assert!(FsStorage::new(root).is_err());
    }

    #[test]
    fn many_successful_activations_drain_deferred_intents_on_owned_drop() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        for revision in 1u32..=64 {
            let bytes = revision.to_le_bytes();
            let item = activation_test_item(&bytes, revision);
            storage
                .activate_verified_item(
                    item,
                    Path::new("artifact.bin"),
                    &activation_staged_data(&bytes, &activation_test_item(&bytes, revision)),
                )
                .unwrap();
        }
        drop(storage);
        let transactions = root.join(FS_ACTIVATION_NAMESPACE).join("transactions");
        assert_eq!(fs::read_dir(transactions).unwrap().count(), 0);
        assert_eq!(
            FsStorage::new(root)
                .unwrap()
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap()
                .item()
                .metadata
                .revision,
            64
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "local APFS activation performance reference; run explicitly in release mode"]
    fn activation_reference_performance() {
        use crate::chunk_storage::hashmap_storage::HashMapStorage;
        use std::{os::unix::fs::MetadataExt, process::Command, time::Instant};

        const ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
        const ENTRY_BYTES: usize = 256 * 1024;
        const CHUNK_COUNT: usize = ARTIFACT_BYTES / ENTRY_BYTES;

        fn p95(samples: &[std::time::Duration]) -> std::time::Duration {
            let mut sorted = samples.to_vec();
            sorted.sort_unstable();
            sorted[(sorted.len() * 95).div_ceil(100).saturating_sub(1)]
        }

        fn milliseconds(samples: &[std::time::Duration]) -> Vec<f64> {
            samples
                .iter()
                .map(|sample| sample.as_secs_f64() * 1_000.0)
                .collect()
        }

        fn worst_campaign_p95(campaigns: &[Vec<std::time::Duration>]) -> std::time::Duration {
            campaigns.iter().map(|samples| p95(samples)).max().unwrap()
        }

        fn allocated_bytes(root: &Path) -> u64 {
            fn visit(path: &Path, seen: &mut HashSet<(u64, u64)>) -> u64 {
                let Ok(metadata) = fs::symlink_metadata(path) else {
                    return 0;
                };
                if metadata.file_type().is_symlink() {
                    return 0;
                }
                if metadata.is_file() {
                    return seen
                        .insert((metadata.dev(), metadata.ino()))
                        .then_some(metadata.blocks().saturating_mul(512))
                        .unwrap_or(0);
                }
                fs::read_dir(path)
                    .into_iter()
                    .flatten()
                    .filter_map(Result::ok)
                    .map(|entry| visit(&entry.path(), seen))
                    .sum()
            }
            visit(root, &mut HashSet::default())
        }

        fn open_reference_storage(root: PathBuf) -> FsStorage {
            FsStorage::with_cache_config(
                root,
                FsStorageCacheConfig {
                    max_chunk_bytes: 1,
                    max_tree_entries: 1,
                },
            )
            .unwrap()
        }

        fn stage_one_chunk(bytes: &[u8], item: &Item, index: usize) -> HashMap<Hash, ChunkData> {
            let chunk = item.chunks[index];
            let offset = item.chunks[..index]
                .iter()
                .map(|chunk| chunk.size as usize)
                .sum::<usize>();
            let end = offset + chunk.size as usize;
            let staged = HashMap::from_iter([(
                chunk.hash,
                ChunkData::Owned(Arc::new(bytes[offset..end].to_vec())),
            )]);
            assert_eq!(
                staged
                    .values()
                    .map(|data| data.as_ref().len())
                    .sum::<usize>(),
                ENTRY_BYTES
            );
            assert!(FS_ACTIVATION_COPY_BUFFER_BYTES + ENTRY_BYTES <= 8 * 1024 * 1024);
            staged
        }

        fn stable_file_item(bytes: &[u8], revision: u32) -> Item {
            let source = HashMapStorage::default();
            let mut item = source
                .prepare_revision(
                    "activation-reference".to_string(),
                    "activation-reference".to_string(),
                    PathBuf::from("artifact.bin"),
                    revision,
                    Some("deterministic 16MiB Fixed256K reference".to_string()),
                    bytes::Bytes::copy_from_slice(bytes),
                    ChunkAlgorithm::Fixed256K,
                )
                .unwrap();
            item.metadata.created = std::time::SystemTime::UNIX_EPOCH;
            item.metadata.updated = std::time::SystemTime::UNIX_EPOCH;
            item
        }

        fn stable_directory_item(files: &[Vec<u8>], revision: u32) -> Item {
            let source = HashMapStorage::default();
            let mut item = source
                .prepare_revision_from_files(
                    "activation-directory-reference".to_string(),
                    "activation-directory-reference".to_string(),
                    PathBuf::from("artifact-dir"),
                    revision,
                    Some("deterministic 64-file Fixed256K reference".to_string()),
                    files
                        .iter()
                        .enumerate()
                        .map(|(index, bytes)| {
                            (
                                PathBuf::from(format!("file-{index:02}.bin")),
                                bytes::Bytes::copy_from_slice(bytes),
                            )
                        })
                        .collect(),
                    ChunkAlgorithm::Fixed256K,
                )
                .unwrap();
            item.metadata.created = std::time::SystemTime::UNIX_EPOCH;
            item.metadata.updated = std::time::SystemTime::UNIX_EPOCH;
            item
        }

        fn run_single_campaign(campaign: usize) -> (Vec<std::time::Duration>, Vec<u64>) {
            let baseline_bytes = (0..CHUNK_COUNT)
                .flat_map(|chunk| std::iter::repeat_n(chunk as u8, ENTRY_BYTES))
                .collect::<Vec<_>>();
            let mut samples = Vec::with_capacity(20);
            let mut allocated = Vec::with_capacity(20);
            for sample in 0..20 {
                let root = temp_path();
                let storage = open_reference_storage(root.clone());
                let baseline = stable_file_item(&baseline_bytes, 1);
                storage
                    .activate_verified_item(
                        baseline.clone(),
                        Path::new("artifact.bin"),
                        &activation_staged_data(&baseline_bytes, &baseline),
                    )
                    .unwrap();
                let mut bytes = baseline_bytes.clone();
                bytes[7 * ENTRY_BYTES..8 * ENTRY_BYTES].fill(if (campaign + sample) % 2 == 0 {
                    0xa5
                } else {
                    0x5a
                });
                let item = stable_file_item(&bytes, 2);
                let staged = stage_one_chunk(&bytes, &item, 7);
                let before = allocated_bytes(&root);
                let started = Instant::now();
                storage
                    .activate_verified_item(item.clone(), Path::new("artifact.bin"), &staged)
                    .unwrap();
                samples.push(started.elapsed());
                allocated.push(allocated_bytes(&root).saturating_sub(before));
                assert_eq!(fs::read(root.join("artifact.bin")).unwrap(), bytes);
                let handle = storage
                    .open_active_revision(Path::new("artifact.bin"))
                    .unwrap()
                    .unwrap();
                assert_eq!(handle.item(), &item);
                let mut active_bytes = Vec::new();
                handle
                    .open_file()
                    .unwrap()
                    .read_to_end(&mut active_bytes)
                    .unwrap();
                assert_eq!(active_bytes, bytes);
                drop(handle);
                drop(storage);
                let restarted = open_reference_storage(root.clone());
                assert_eq!(
                    restarted
                        .open_active_revision(Path::new("artifact.bin"))
                        .unwrap()
                        .unwrap()
                        .item(),
                    &item
                );
                drop(restarted);
                assert_eq!(
                    fs::read_dir(root.join(FS_ACTIVATION_NAMESPACE).join("transactions"))
                        .unwrap()
                        .count(),
                    0
                );
                assert_eq!(
                    fs::read_dir(root.join(FS_ACTIVATION_NAMESPACE).join("staging"))
                        .unwrap()
                        .count(),
                    0
                );
            }
            println!(
                "single_campaign_{campaign}_ms={:?} allocated_delta_bytes={allocated:?}",
                milliseconds(&samples)
            );
            (samples, allocated)
        }

        fn run_directory_campaign(campaign: usize) -> (Vec<std::time::Duration>, Vec<u64>) {
            let baseline_files = (0..CHUNK_COUNT)
                .map(|index| vec![index as u8; ENTRY_BYTES])
                .collect::<Vec<_>>();
            let mut samples = Vec::with_capacity(20);
            let mut allocated = Vec::with_capacity(20);
            for sample in 0..20 {
                let root = temp_path();
                let storage = open_reference_storage(root.clone());
                let baseline = stable_directory_item(&baseline_files, 1);
                let baseline_bytes = baseline_files.concat();
                storage
                    .activate_verified_item(
                        baseline.clone(),
                        Path::new("artifact-dir"),
                        &activation_staged_data(&baseline_bytes, &baseline),
                    )
                    .unwrap();
                let mut files = baseline_files.clone();
                files[7].fill(if (campaign + sample) % 2 == 0 {
                    0xa5
                } else {
                    0x5a
                });
                let item = stable_directory_item(&files, 2);
                let bytes = files.concat();
                let staged = stage_one_chunk(&bytes, &item, 7);
                let before = allocated_bytes(&root);
                let started = Instant::now();
                storage
                    .activate_verified_item(item.clone(), Path::new("artifact-dir"), &staged)
                    .unwrap();
                samples.push(started.elapsed());
                allocated.push(allocated_bytes(&root).saturating_sub(before));
                let handle = storage
                    .open_active_revision(Path::new("artifact-dir"))
                    .unwrap()
                    .unwrap();
                assert_eq!(handle.item(), &item);
                for (index, expected) in files.iter().enumerate() {
                    let relative = PathBuf::from(format!("file-{index:02}.bin"));
                    assert_eq!(
                        fs::read(root.join("artifact-dir").join(&relative)).unwrap(),
                        *expected
                    );
                    let mut active_bytes = Vec::new();
                    handle
                        .open_entry(&relative)
                        .unwrap()
                        .read_to_end(&mut active_bytes)
                        .unwrap();
                    assert_eq!(active_bytes, *expected);
                }
                drop(handle);
                drop(storage);
                let restarted = open_reference_storage(root.clone());
                assert_eq!(
                    restarted
                        .open_active_revision(Path::new("artifact-dir"))
                        .unwrap()
                        .unwrap()
                        .item(),
                    &item
                );
                drop(restarted);
                assert_eq!(
                    fs::read_dir(root.join(FS_ACTIVATION_NAMESPACE).join("transactions"))
                        .unwrap()
                        .count(),
                    0
                );
                assert_eq!(
                    fs::read_dir(root.join(FS_ACTIVATION_NAMESPACE).join("staging"))
                        .unwrap()
                        .count(),
                    0
                );
            }
            println!(
                "directory_campaign_{campaign}_ms={:?} allocated_delta_bytes={allocated:?}",
                milliseconds(&samples)
            );
            (samples, allocated)
        }

        assert!(
            !cfg!(debug_assertions),
            "activation performance reference must run with --release"
        );
        let git_revision = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let os = Command::new("sw_vers")
            .output()
            .ok()
            .map(|output| String::from_utf8_lossy(&output.stdout).replace('\n', "; "))
            .unwrap_or_else(|| "unknown".to_string());
        println!(
            "activation_reference_identity git={git_revision} source_blake3={} package={} arch={} os={os}",
            blake3::hash(include_bytes!("fs_storage.rs")),
            env!("CARGO_PKG_VERSION"),
            std::env::consts::ARCH,
        );

        let single_campaigns = [run_single_campaign(1), run_single_campaign(2)];
        let directory_campaigns = [run_directory_campaign(1), run_directory_campaign(2)];
        let single_p95 = single_campaigns
            .iter()
            .map(|(samples, _)| p95(samples))
            .max()
            .unwrap();
        let directory_p95 = directory_campaigns
            .iter()
            .map(|(samples, _)| p95(samples))
            .max()
            .unwrap();
        let single_amplification_limit = (ARTIFACT_BYTES as u64 * 11).div_ceil(10) + 1024 * 1024;
        let directory_amplification_limit = (ENTRY_BYTES as u64 * 11).div_ceil(10) + 1024 * 1024;
        assert!(single_campaigns
            .iter()
            .flat_map(|(_, allocated)| allocated)
            .all(|delta| *delta <= single_amplification_limit));
        assert!(directory_campaigns
            .iter()
            .flat_map(|(_, allocated)| allocated)
            .all(|delta| *delta <= directory_amplification_limit));

        let mut selector_campaigns = Vec::with_capacity(3);
        let mut direct_campaigns = Vec::with_capacity(3);
        let mut indexed_campaigns = Vec::with_capacity(3);
        let mut handle_campaigns = Vec::with_capacity(3);
        let mut restart_campaigns = Vec::with_capacity(3);
        for campaign in 1..=3 {
            let component_root = temp_path();
            let storage = open_reference_storage(component_root.clone());
            let bytes = (0..CHUNK_COUNT)
                .flat_map(|chunk| std::iter::repeat_n(chunk as u8, ENTRY_BYTES))
                .collect::<Vec<_>>();
            let item = stable_file_item(&bytes, 1);
            storage
                .activate_verified_item(
                    item.clone(),
                    Path::new("artifact.bin"),
                    &activation_staged_data(&bytes, &item),
                )
                .unwrap();
            let identity = storage
                .activation_destination_identity(Path::new("artifact.bin"))
                .unwrap();
            let active = storage.active_refs.read().unwrap()[&identity].clone();
            let active_path = storage.active_ref_path(&identity);

            let mut selector_samples = Vec::with_capacity(20);
            for _ in 0..20 {
                let started = Instant::now();
                storage
                    .write_activation_record(
                        &active_path,
                        FS_ACTIVATION_ACTIVE_SCHEMA,
                        ActivationRecordKind::Active,
                        active.as_ref(),
                    )
                    .unwrap();
                selector_samples.push(started.elapsed());
            }
            let handle = storage
                .open_active_revision(Path::new("artifact.bin"))
                .unwrap()
                .unwrap();
            let mut direct_samples = Vec::with_capacity(100);
            let mut indexed_samples = Vec::with_capacity(100);
            for _ in 0..100 {
                let started = Instant::now();
                let mut file = handle.open_file().unwrap();
                let mut read = Vec::with_capacity(ARTIFACT_BYTES);
                file.read_to_end(&mut read).unwrap();
                assert_eq!(read, bytes);
                direct_samples.push(started.elapsed());

                let started = Instant::now();
                for index in 0..CHUNK_COUNT {
                    assert_eq!(
                        storage
                            .get_chunk_by_index(&item.metadata.root.hash, index as u32)
                            .unwrap(),
                        bytes[index * ENTRY_BYTES..(index + 1) * ENTRY_BYTES]
                    );
                }
                indexed_samples.push(started.elapsed());
            }
            let mut handle_samples = Vec::with_capacity(1_000);
            for _ in 0..1_000 {
                let started = Instant::now();
                assert!(storage
                    .open_active_revision(Path::new("artifact.bin"))
                    .unwrap()
                    .is_some());
                handle_samples.push(started.elapsed());
            }
            assert!(storage.handles.read().unwrap().len() * CHUNK_SIZE * 8 <= 8 * 1024 * 1024);
            assert!(
                storage.immutable_readers.lock().unwrap().entries.len()
                    <= DEFAULT_IMMUTABLE_READER_ENTRIES
            );
            drop(handle);
            drop(storage);

            let mut restart_samples = Vec::with_capacity(5);
            for _ in 0..5 {
                let started = Instant::now();
                let restarted = open_reference_storage(component_root.clone());
                restart_samples.push(started.elapsed());
                assert!(restarted
                    .open_active_revision(Path::new("artifact.bin"))
                    .unwrap()
                    .is_some());
                drop(restarted);
            }
            println!(
                "component_campaign_{campaign}_selector_ms={:?} direct_ms={:?} indexed64_ms={:?} handle_open_ms={:?} restart_ms={:?}",
                milliseconds(&selector_samples),
                milliseconds(&direct_samples),
                milliseconds(&indexed_samples),
                milliseconds(&handle_samples),
                milliseconds(&restart_samples),
            );
            selector_campaigns.push(selector_samples);
            direct_campaigns.push(direct_samples);
            indexed_campaigns.push(indexed_samples);
            handle_campaigns.push(handle_samples);
            restart_campaigns.push(restart_samples);
        }
        let selector_p95 = worst_campaign_p95(&selector_campaigns);
        let direct_p95 = worst_campaign_p95(&direct_campaigns);
        let indexed_p95 = worst_campaign_p95(&indexed_campaigns);
        let handle_p95 = worst_campaign_p95(&handle_campaigns);
        let restart_p95 = worst_campaign_p95(&restart_campaigns);
        println!(
            "activation_reference_p95_ms single={:.3} directory={:.3} selector={:.3} direct={:.3} indexed64={:.3} handle={:.6} restart={:.3} single_amplification_limit_bytes={single_amplification_limit} directory_amplification_limit_bytes={directory_amplification_limit}",
            single_p95.as_secs_f64() * 1_000.0,
            directory_p95.as_secs_f64() * 1_000.0,
            selector_p95.as_secs_f64() * 1_000.0,
            direct_p95.as_secs_f64() * 1_000.0,
            indexed_p95.as_secs_f64() * 1_000.0,
            handle_p95.as_secs_f64() * 1_000.0,
            restart_p95.as_secs_f64() * 1_000.0,
        );

        assert!(single_p95.as_millis() < 100);
        assert!(directory_p95.as_millis() < 200);
        assert!(selector_p95.as_millis() < 30);
        assert!(handle_p95.as_millis() < 1);
        assert!(direct_p95.as_millis() < 10);
        assert!(indexed_p95.as_millis() < 50);
        assert!(restart_p95.as_millis() < 120);
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
