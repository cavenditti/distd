use std::{
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
    chunks::{ChunkInfo, CHUNK_SIZE},
    error::{Error, InvalidParameter},
    hash::{hash as do_hash, Hash, HashTreeCapable},
    item::{Item, Name as ItemName},
    utils::settings::cache_dir,
};

use super::{ChunkStorage, Node};

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
        self.buf_writer
            .write_all(chunk)
            .map_err(Error::IoError)?;
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
#[derive(Default, Serialize, Deserialize)]
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
    dirty: bool,
}

impl FsStorageState {
    fn mark_dirty(&mut self) {
        self.dirty = true;
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
        tracing::debug!("Created path {:?}", full_path.parent().unwrap_or(&full_path));
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
        self.data.entry(chunk_info.hash).or_default().push(Arc::new(ifc));
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
    fn store_link(&mut self, hash: Hash, left: Arc<Node>, right: Arc<Node>) -> Result<Arc<Node>, StorageError> {
        let size = left.size() + right.size();
        let res = self
            .links
            .entry(hash)
            .or_insert_with(|| Arc::new(Node::Parent { hash, left, right, size }))
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
pub struct FsStorage {
    inner: RwLock<FsStorageState>,
    handles: RwLock<HashMap<PathBuf, Arc<Mutex<Handle>>>>,
}

impl Default for FsStorage {
    fn default() -> Self {
        Self {
            inner: RwLock::new(FsStorageState::default()),
            handles: RwLock::new(HashMap::default()),
        }
    }
}

impl FsStorage {
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
            handle
                .lock()
                .unwrap()
                .flush()
                .inspect_err(|e| tracing::error!("Cannot flush handle {}: {e}", path.to_string_lossy()))?;
        }
        Ok(())
    }

    fn flush_data_for_hash(&self, hash: &Hash) -> Result<(), Error> {
        for path in self.data_paths_for_hash(hash) {
            if let Some(handle) = self.handle_for_path(&path) {
                handle
                    .lock()
                    .unwrap()
                    .flush()
                    .inspect_err(|e| tracing::error!("Cannot flush infile chunk handle {}: {e}", path.to_string_lossy()))?;
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
                    tracing::warn!("Cannot deserialize FsStorage persistence data: {e}; starting fresh");
                }
                Ok(mut s) => {
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
                            Node::Parent { hash, left, right, .. } => {
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

    /// Pre-allocate a single `ChunkInfo` slot on disk
    pub fn pre_allocate_chunk(
        &self,
        path: &Path,
        chunk_info: &ChunkInfo,
        offset: u64,
    ) -> Result<(), Error> {
        self.inner.write().unwrap().pre_allocate_chunk(path, chunk_info, offset)?;
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
        inner.items.insert(item.clone());
        drop(inner);
        self.ensure_handle(&path)?;
        self.persist()
    }

    /// Remove references to a file from storage (does not delete the file from disk)
    pub fn remove(&self, item: Item) -> Result<(), Error> {
        let mut inner = self.inner.write().unwrap();
        let path = inner.item_path(&item)?;
        let item = inner
            .items
            .remove(&item)
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
            return linked;
        }

        self.flush_data_for_hash(hash)
            .inspect_err(|e| tracing::error!("Cannot flush chunk data for {hash}: {e}"))
            .ok()?;

        self.inner.read().unwrap().get_data(hash)
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
            let handle = self
                .handle_for_path(&infile_chunk.path)
                .ok_or(StorageError::ChunkInsertError)?;
            infile_chunk
                .write(&hash, chunk, &mut handle.lock().unwrap())
                .inspect(|()| tracing::trace!("Written infile chunk {hash} to {}", infile_chunk.path.to_string_lossy()))
                .inspect_err(|e| tracing::error!("Cannot write infile chunk to {}: {e}", infile_chunk.path.to_string_lossy()))
                .map_err(|_| StorageError::ChunkInsertError)?;
        }

        self.inner.write().unwrap().mark_dirty();
        Ok(Arc::new(Node::Stored {
            hash,
            data: Arc::new(chunk.to_vec()),
        }))
    }

    fn store_link(&self, hash: Hash, left: Arc<Node>, right: Arc<Node>) -> Result<Arc<Node>, StorageError> {
        self.inner.write().unwrap().store_link(hash, left, right)
    }

    fn create_item(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        file: bytes::Bytes,
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        tracing::debug!("Create item {name} with path {path:?}");
        let mut inner = self.inner.write().unwrap();
        let path = inner.path(&path);
        create_dir_all(path.parent().ok_or(Error::MissingData)?)?;
        inner.pre_allocate_bytes(&path, &file)?;
        drop(inner);
        self.ensure_handle(&path)?;
        tracing::info!("Preallocated on disk {:?}", path);

        // Build the hash tree using interior HashTreeCapable
        let hash_tree = self.compute_tree(file.as_ref())?;
        let mut inner = self.inner.write().unwrap();
        let item = Item::new(name, path, revision, description, &hash_tree);
        tracing::debug!("New item: {item}");
        inner.items.insert(item.clone());
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
    ) -> Result<Item, Error>
    where
        Self: Sized,
    {
        tracing::debug!("Create item {name} with path {path:?}");
        let mut inner = self.inner.write().unwrap();
        let path = inner.path(&path);
        inner.pre_allocate(&path, &root.flatten_with_sizes()?)?;
        drop(inner);
        self.ensure_handle(&path)?;
        tracing::info!("Preallocated on disk {:?}", path);
        let mut inner = self.inner.write().unwrap();
        let item = Item::new(name, path, revision, description, &root);
        tracing::debug!("New item: {item}");
        inner.items.insert(item.clone());
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
    ) -> Result<Item, crate::error::Error>
    where
        Self: Sized,
        T: Stream<Item = Node> + std::marker::Unpin,
    {
        let path = self.path(&path);
        self.ensure_handle(&path)?;
        tracing::trace!("Receiving item at '{}'", path.to_string_lossy());
        let mut i = 0;
        let mut o = 0u64;
        let mut last: Option<Arc<Node>> = None;

        while let Some(node) = stream.next().await {
            if let s_n @ Node::Stored { .. } = &node {
                tracing::trace!(
                    "Preallocating {} bytes in {}@'{}'",
                    s_n.size(),
                    o,
                    path.to_string_lossy()
                );
                self.pre_allocate_chunk(&path, &s_n.chunk_info(), o)?;
                o += s_n.size();
            }
            last = Some(self.try_fill_in(&node)?);
            i += 1;
        }

        let last = last.ok_or(StorageError::TreeReconstruct)?;
        tracing::info!("Reconstructed {i} nodes with {} bytes total", last.size());
        let item = Item::new(name, path, revision, description, &last);
        let mut inner = self.inner.write().unwrap();
        inner.items.insert(item.clone());
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
        chunks::CHUNK_SIZE,
        hash::hash as do_hash,
        item::tests::{make_ones_item, new_dummy_item},
        utils::testing::temp_path,
    };
    use std::str::FromStr;

    use test_log::test;

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

        let stored = storage.get(&item.metadata.root.hash).unwrap().clone_data().unwrap();

        // reported storage size is deduplicated
        assert_eq!(stored.len(), 1_000_000);
        for b in stored {
            assert_eq!(b, 1u8);
        }

        // check for data on disk to match the expected one
        let file = std::fs::read(item.metadata.path).unwrap();
        assert_eq!(file.len(), 1_000_000);
        for b in file {
            assert_eq!(b, 1u8);
        }
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
            let stored = storage.get(&item_hash.unwrap()).unwrap().clone_data().unwrap();

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
            let stored = storage.get(&item.metadata.root.hash).unwrap().clone_data().unwrap();
            assert_eq!(stored.len(), 1_000_000);
            for b in stored {
                assert_eq!(b, 1u8);
            }
        });
    }
}
