use std::{
    collections::VecDeque,
    fs::{self, create_dir_all, remove_file, File},
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
    error::{Error, InvalidParameter},
    hash::{hash as do_hash, merge_hashes, Hash, HashTreeCapable},
    item::{FileEntry, Item, Manifest, Name as ItemName},
    utils::settings::cache_dir,
};

use super::{ChunkStorage, Node};

const DEFAULT_CHUNK_CACHE_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_TREE_CACHE_ENTRIES: usize = 65_536;

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
    entries: HashMap<Hash, Arc<Vec<u8>>>,
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
        }
    }

    fn get(&mut self, hash: &Hash) -> Option<Arc<Vec<u8>>> {
        let data = self.entries.get(hash)?.clone();
        self.order.push_back(*hash);
        Some(data)
    }

    fn insert(&mut self, hash: Hash, data: Arc<Vec<u8>>) {
        if self.max_bytes == 0 {
            return;
        }

        let data_len = data.len();
        if data_len > self.max_bytes {
            return;
        }

        if let Some(old) = self.entries.insert(hash, data) {
            self.total_bytes = self.total_bytes.saturating_sub(old.len());
        }

        self.total_bytes = self.total_bytes.saturating_add(data_len);
        self.order.push_back(hash);

        while self.total_bytes > self.max_bytes {
            let Some(oldest_hash) = self.order.pop_front() else {
                break;
            };
            let remove = self
                .entries
                .get(&oldest_hash)
                .map(|current| Arc::strong_count(current) == 1)
                .unwrap_or(false);
            if remove {
                if let Some(old) = self.entries.remove(&oldest_hash) {
                    self.total_bytes = self.total_bytes.saturating_sub(old.len());
                }
            }
        }
    }
}

pub fn open_file(path: &Path) -> Result<File, Error> {
    File::options()
        .create(true)
        .write(true)
        .append(false)
        .truncate(false)
        .open(path)
        .inspect_err(|e| tracing::error!("Cannot create file at {:?}: {}", path, e))
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
}

impl FsStorageState {
    fn mark_dirty(&mut self) {
        self.dirty = true;
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
    }

    fn remove_item(&mut self, item: &Item) -> bool {
        let removed = self.items.remove(item);
        if removed {
            self.rebuild_indexes();
        }
        removed
    }

    /// Returns the stored path of any path relative to root
    fn path(&self, path: &Path) -> PathBuf {
        if path.starts_with(&self.root) {
            path.to_path_buf()
        } else {
            crate::utils::path::join(&self.root, path)
        }
    }

    /// Full on-disk path for an item
    fn item_path(&self, item: &Item) -> Result<PathBuf, Error> {
        let full_path = self.root.join(
            item.metadata
                .path
                .strip_prefix("/")
                .unwrap_or(&item.metadata.path),
        );
        create_dir_all(full_path.parent().unwrap_or(&full_path))?;
        tracing::debug!(
            "Created path {:?}",
            full_path.parent().unwrap_or(&full_path)
        );
        Ok(full_path)
    }

    /// Retrieve a stored `Node` by reading from disk
    fn get_data(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.data
            .get(hash)
            .and_then(|entries| entries.first())
            .and_then(|x| Node::try_from(x).ok())
            .map(Arc::new)
    }

    fn item_for_root(&self, root: &Hash) -> Option<Item> {
        self.root_index.get(root).cloned()
    }

    /// Atomically persist state to disk (write to *.tmp then rename)
    fn persist(&mut self) -> Result<(), Error> {
        let buf = bitcode::serialize(self)
            .inspect_err(|e| tracing::error!("Serialization error: {}", e))
            .map_err(InvalidParameter::from)?;
        let tmp = self.persistance_path.with_extension("tmp");
        fs::write(&tmp, &buf)
            .inspect_err(|e| tracing::error!("Cannot write persistence tmp file: {}", e))?;
        fs::rename(&tmp, &self.persistance_path)
            .inspect_err(|e| tracing::error!("Cannot rename persistence file: {}", e))?;
        self.dirty = false;
        Ok(())
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
        let size = left.size() + right.size();
        let res = self
            .links
            .entry(hash)
            .or_insert_with(|| {
                Arc::new(Node::Parent {
                    hash,
                    left,
                    right,
                    size,
                })
            })
            .clone();
        self.mark_dirty();
        Ok(res)
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
    handles: RwLock<HashMap<PathBuf, Arc<Mutex<Handle>>>>,
    chunk_cache: Mutex<HotChunkCache>,
    tree_cache: Mutex<HotNodeCache>,
}

impl Default for FsStorage {
    fn default() -> Self {
        Self {
            inner: RwLock::new(FsStorageState::default()),
            handles: RwLock::new(HashMap::default()),
            chunk_cache: Mutex::new(HotChunkCache::new(DEFAULT_CHUNK_CACHE_BYTES)),
            tree_cache: Mutex::new(HotNodeCache::new(DEFAULT_TREE_CACHE_ENTRIES)),
        }
    }
}

impl FsStorage {
    fn artifact_entry_path(root: &Path, relative_path: &Path) -> PathBuf {
        relative_path
            .components()
            .fold(root.to_path_buf(), |mut acc, component| {
                acc.push(component.as_os_str());
                acc
            })
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

    fn cached_chunk_data(&self, hash: &Hash) -> Option<Arc<Vec<u8>>> {
        self.chunk_cache.lock().unwrap().get(hash)
    }

    fn cached_tree_node(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.tree_cache.lock().unwrap().get(hash)
    }

    fn cache_chunk_data(&self, hash: Hash, data: Arc<Vec<u8>>) -> Arc<Vec<u8>> {
        self.chunk_cache.lock().unwrap().insert(hash, data.clone());
        data
    }

    fn cache_tree_node(&self, hash: Hash, node: Arc<Node>) -> Arc<Node> {
        self.tree_cache.lock().unwrap().insert(hash, node.clone());
        node
    }

    fn read_chunk_data(&self, hash: &Hash) -> Option<Arc<Vec<u8>>> {
        if let Some(cached) = self.cached_chunk_data(hash) {
            return Some(cached);
        }

        self.flush_data_for_hash(hash).ok()?;

        let entry = {
            let inner = self.inner.read().unwrap();
            inner
                .data
                .get(hash)
                .and_then(|entries| entries.first())
                .cloned()
        }?;

        let node = Node::try_from(&entry).ok()?;
        let data = node.stored_data()?;
        Some(self.cache_chunk_data(*hash, data))
    }

    fn stored_node(&self, hash: &Hash) -> Option<Arc<Node>> {
        let data = self.read_chunk_data(hash)?;
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
            let full_path = Self::artifact_entry_path(root_path, relative_path);
            create_dir_all(full_path.parent().ok_or(Error::MissingData)?)?;

            let start = next_chunk_index;
            let mut offset = 0u64;
            for (chunk_offset, length) in chunk_algorithm.chunk_boundaries(data.as_ref()) {
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
        received_chunks: &mut VecDeque<Vec<u8>>,
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
                    self.read_chunk_data(&chunk_info.hash)
                        .ok_or_else(|| Error::Storage(StorageError::TreeReconstruct))?;
                } else {
                    let data = received_chunks
                        .pop_front()
                        .ok_or_else(|| Error::Storage(StorageError::TreeReconstruct))?;
                    if do_hash(&data) != chunk_info.hash {
                        return Err(Error::Storage(StorageError::TreeReconstruct));
                    }
                    self.cache_chunk_data(chunk_info.hash, Arc::new(data));
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
    ) -> Result<(), Error> {
        let mut unique_hashes = HashSet::default();
        let mut activation_paths = Vec::new();
        let mut activation_ranges = Vec::new();

        if manifest.entries.is_empty() {
            activation_paths.push(item_root.to_path_buf());
            activation_ranges.push((0usize, chunks.len()));
            unique_hashes.extend(chunks.iter().map(|chunk| chunk.hash));
        } else {
            let mut ensured_parents = HashSet::default();
            for entry in &manifest.entries {
                let relative_path = PathBuf::from(&entry.relative_path);
                let full_path = Self::artifact_entry_path(item_root, &relative_path);
                let parent = full_path.parent().ok_or(Error::MissingData)?;
                if ensured_parents.insert(parent.to_path_buf()) {
                    create_dir_all(parent)?;
                }

                let start = entry.chunk_range.0 as usize;
                let end = entry.chunk_range.1 as usize;
                activation_paths.push(full_path);
                activation_ranges.push((start, end));
                unique_hashes.extend(chunks.iter().take(end).skip(start).map(|chunk| chunk.hash));
            }
        }

        {
            let mut inner = self.inner.write().unwrap();
            for (path, (start, end)) in activation_paths
                .iter()
                .zip(activation_ranges.iter().copied())
            {
                inner.pre_allocate(path, &chunks[start..end])?;
            }
        }

        for hash in unique_hashes {
            let data = self
                .read_chunk_data(&hash)
                .ok_or_else(|| Error::Storage(StorageError::TreeReconstruct))?;
            self.store_chunk(hash, data.as_ref()).map_err(Error::from)?;
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

    fn data_paths_for_hash(&self, hash: &Hash) -> Vec<PathBuf> {
        self.inner
            .read()
            .unwrap()
            .data
            .get(hash)
            .map(|entries| {
                entries
                    .iter()
                    .map(|entry| entry.path.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
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
        if chunk_infos.len() != manifest.chunk_count as usize {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }

        let stored_path = self.path(&path);
        let available_hashes: HashSet<Hash> = local_hashes.iter().copied().collect();
        let mut received_chunks: VecDeque<Vec<u8>> = received_chunks.into();
        let (chunks, root) = if manifest.entries.is_empty() {
            let mut partials = Vec::with_capacity(chunk_infos.len());
            let mut chunks = Vec::with_capacity(chunk_infos.len());

            for chunk_info in chunk_infos.iter().copied() {
                chunks.push(chunk_info);

                let node = if available_hashes.contains(&chunk_info.hash) {
                    self.read_chunk_data(&chunk_info.hash)
                        .ok_or_else(|| Error::Storage(StorageError::TreeReconstruct))?;
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
                    self.cache_chunk_data(chunk_info.hash, Arc::new(data));
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
            )?
        };

        if !received_chunks.is_empty() {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }

        if root.hash() != &manifest.root_hash || root.size() != manifest.total_size {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }

        self.activate_staged_chunks(&stored_path, manifest, &chunks)?;

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
        inner.items.insert(item.clone());
        drop(inner);
        self.persist()?;
        Ok(item)
    }

    fn flush_data_for_hash(&self, hash: &Hash) -> Result<(), Error> {
        for path in self.data_paths_for_hash(hash) {
            if let Some(handle) = self.handle_for_path(&path) {
                handle.lock().unwrap().flush().inspect_err(|e| {
                    tracing::error!(
                        "Cannot flush infile chunk handle {}: {e}",
                        path.to_string_lossy()
                    )
                })?;
            }
        }
        Ok(())
    }

    fn persist(&self) -> Result<(), Error> {
        self.flush_handles()?;
        self.inner.write().unwrap().persist()
    }

    /// Create a new `FsStorage` with a root path.
    ///
    /// If persistence data exists it will be reloaded; otherwise an empty storage is created.
    /// Falls back to an empty storage (with a warning) if persistence data is corrupt.
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self::with_cache_config(root, FsStorageCacheConfig::default())
    }

    #[must_use]
    pub fn with_cache_config(root: PathBuf, cache_config: FsStorageCacheConfig) -> Self {
        let persistance_dir = cache_dir().join("chunk_storage").join("fs_storage");
        let persistance_path = persistance_dir.join(root.to_string_lossy().replace('/', "___"));
        create_dir_all(&persistance_dir).unwrap();
        create_dir_all(&root)
            .inspect(|()| tracing::info!("Created root path '{}'", root.to_string_lossy()))
            .unwrap();

        if let Ok(file) = std::fs::read(&persistance_path) {
            tracing::debug!("FsStorage data found, loading…");

            match bitcode::deserialize::<FsStorageState>(&file) {
                Err(e) => {
                    tracing::warn!(
                        "Cannot deserialize FsStorage persistence data: {e}; starting fresh"
                    );
                }
                Ok(mut s) => {
                    s.rebuild_indexes();
                    let mut handles_map = HashMap::default();
                    for hash in s.data.keys().copied().collect::<Vec<_>>() {
                        if let Some(entries) = s.data.get(&hash) {
                            for entry in entries {
                                handles_map
                                    .entry(entry.path.clone())
                                    .or_insert(Handle::new(&entry.path).unwrap());
                            }
                        }
                    }

                    // Re-link parent nodes after deserialization
                    fn node_relink(
                        s: &mut FsStorageState,
                        already_processed: &mut HashMap<Hash, Arc<Node>>,
                        node: &Arc<Node>,
                    ) -> Option<Arc<Node>> {
                        if already_processed.contains_key(node.hash()) {
                            return already_processed.get(node.hash()).cloned();
                        }
                        match node.as_ref() {
                            Node::Parent {
                                hash, left, right, ..
                            } => {
                                let n = Arc::new(Node::Parent {
                                    hash: *node.hash(),
                                    size: node.size(),
                                    left: node_relink(s, already_processed, left)?,
                                    right: node_relink(s, already_processed, right)?,
                                });
                                s.links.insert(*hash, n.clone());
                                already_processed.insert(*hash, n.clone());
                                Some(n)
                            }
                            Node::Skipped { hash, .. } => {
                                let n = s.get_data(hash)?;
                                already_processed.insert(*hash, n.clone());
                                Some(n)
                            }
                            Node::Stored { .. } => panic!("Nodes in links should never be Stored"),
                        }
                    }

                    let mut already_processed = HashMap::default();
                    let mut old_links = s.links.clone();
                    const MAX_ITER: u32 = 10;
                    let mut i: u32 = 0;
                    while !old_links.is_empty() && i <= MAX_ITER {
                        if i % 5 == 0 {
                            tracing::trace!(
                                "Trying to fill-in old nodes: {} nodes remaining",
                                old_links.len()
                            );
                        }
                        i += 1;
                        for n in old_links.clone().values() {
                            node_relink(&mut s, &mut already_processed, n)
                                .map(|n| old_links.remove(n.hash()));
                        }
                    }

                    if old_links.is_empty() {
                        tracing::debug!("Loaded FsStorage.");
                        return Self {
                            inner: RwLock::new(s),
                            handles: RwLock::new(
                                handles_map
                                    .into_iter()
                                    .map(|(path, handle)| (path, Arc::new(Mutex::new(handle))))
                                    .collect(),
                            ),
                            chunk_cache: Mutex::new(HotChunkCache::new(
                                cache_config.max_chunk_bytes,
                            )),
                            tree_cache: Mutex::new(HotNodeCache::new(
                                cache_config.max_tree_entries,
                            )),
                        };
                    }
                    tracing::debug!("Cannot reload FsStorage; starting fresh");
                }
            }
        } else {
            tracing::debug!("No previous valid FsStorage data found, creating a new one");
        }

        Self {
            inner: RwLock::new(FsStorageState {
                root,
                persistance_path,
                ..Default::default()
            }),
            handles: RwLock::new(HashMap::default()),
            chunk_cache: Mutex::new(HotChunkCache::new(cache_config.max_chunk_bytes)),
            tree_cache: Mutex::new(HotNodeCache::new(cache_config.max_tree_entries)),
        }
    }

    /// Returns the stored path of any path relative to root
    #[must_use]
    pub fn path(&self, path: &Path) -> PathBuf {
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
        let full_path = inner.path(path);
        inner
            .items
            .iter()
            .filter(|item| inner.path(&item.metadata.path) == full_path)
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
                .map(|data| (*data).clone());
        }

        let node = self.get(root)?;
        let leaves = node.flatten_iter().ok()?;
        leaves.get(index as usize).map(|arc| (**arc).clone())
    }

    fn store_chunk(&self, hash: Hash, chunk: &[u8]) -> Result<Arc<Node>, StorageError> {
        let infile_chunks = {
            let inner = self.inner.read().unwrap();
            inner
                .data
                .get(&hash)
                .cloned()
                .ok_or(StorageError::ChunkInsertError)?
        };

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
        let stored_path = inner.path(&path);
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
        tracing::info!("Preallocated on disk {:?}", stored_path);

        let (hash_tree, chunks) = self.build_compact_tree(file.as_ref(), chunk_algorithm)?;
        let mut inner = self.inner.write().unwrap();
        let item = Item::make(
            name,
            path,
            revision,
            description,
            hash_tree.chunk_info(),
            chunks,
            chunk_algorithm,
        )?;
        tracing::debug!("New item: {item}");
        inner.insert_item(item.clone());
        drop(inner);
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

        let item_root = self.path(&path);
        create_dir_all(&item_root)?;
        let (hash_tree, chunks, entries) =
            self.build_compact_tree_from_files(&item_root, &files, chunk_algorithm)?;
        let mut inner = self.inner.write().unwrap();
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
        tracing::debug!("New item: {item}");
        inner.insert_item(item.clone());
        drop(inner);
        self.persist()?;
        Ok(item)
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
        let stored_path = inner.path(&path);
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
        let stored_path = self.path(&path);
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
        let storage = FsStorage::new(tempdir.clone());

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
        let storage = FsStorage::new(tempdir.clone());

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
        let storage = FsStorage::new(tempdir.clone());
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
        );
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
        let storage = FsStorage::new(tempdir.clone());
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
        let storage = FsStorage::new(tempdir.clone());
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
        let storage = FsStorage::new(tempdir.clone());
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
        let received_chunks: Vec<Vec<u8>> = v2
            .chunks(CHUNK_SIZE)
            .enumerate()
            .filter(|(index, _)| matches!(*index, 2 | 6))
            .map(|(_, chunk)| chunk.to_vec())
            .collect();

        let item = storage
            .receive_sync_item(
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
    }

    #[tokio::test]
    async fn fs_storage_receive_sync_item_non_power_of_two_round_trip() {
        use crate::chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage};
        use crate::item::Manifest;

        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone());
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
    fn fs_storage_persistance() {
        // create storage in a temporary directory
        let tempdir = temp_path();

        let item: Option<Item>;
        let item_hash: Option<Hash>;

        // create storage and let it go out of scope
        {
            let storage = FsStorage::new(tempdir.clone());

            // save item and hash
            item = Some(new_dummy_item::<FsStorage, 1u8, 1_000_000>(&storage).unwrap());
            item_hash = Some(item.as_ref().unwrap().metadata.root.hash);
            println!("Created item: {item:?}");
            println!("Item hash: {}", item_hash.unwrap());

            print_fsstorage(&storage);
        }

        // Then re-create storage and retrieve the data
        println!("Reloading storage");
        let storage = FsStorage::new(tempdir.clone());
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

    #[cfg(feature = "nightly-bench")]
    extern crate test;

    #[cfg(feature = "nightly-bench")]
    #[bench]
    fn bench_fs_storage(b: &mut test::Bencher) {
        let tempdir = temp_path();
        let storage = FsStorage::new(tempdir.clone());
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
