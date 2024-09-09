use std::{
    collections::{HashMap, HashSet},
    fs::{create_dir_all, remove_file, File},
    io::{Read, Seek, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc, RwLock},
};

use anyhow::Error;
use blake3::Hash;

use crate::{
    chunks::{ChunkInfo, CHUNK_SIZE},
    hash::hash as do_hash,
    item::{Item, ItemName},
};

use super::{ChunkStorage, StoredChunkRef};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Path and offset of a chunk in a file
struct InFileChunkPaths {
    pub path: PathBuf,
    pub offset: u64,
}

/// Chunk stored in multiple files
/// Basically ref-counting on items paths
#[derive(Debug, Clone)]
struct InFileChunk {
    pub info: ChunkInfo,
    pub paths: HashSet<InFileChunkPaths>,
    pub populated: Arc<AtomicBool>,
    //buf_reader: Arc<Mutex<Option<BufReader<File>>>>,
}

impl TryFrom<InFileChunk> for StoredChunkRef {
    type Error = Error;

    /// Try to read the chunk from the file at first path
    ///
    /// If the chunk is not populated, it will return an error
    fn try_from(value: InFileChunk) -> Result<Self, Self::Error> {
        if !value.populated.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::msg("missing data"));
        }
        let first_path = value.paths.iter().next().ok_or(Error::msg("empty"))?;
        let mut file = File::open(&first_path.path).map_err(Error::msg)?;
        file.seek(std::io::SeekFrom::Start(first_path.offset))
            .map_err(Error::msg)?;

        let mut buf = vec![0u8; value.info.size as usize];

        file.read_exact(&mut buf).map_err(Error::msg)?;

        Ok(StoredChunkRef::Stored {
            hash: value.info.hash,
            data: Arc::new(buf),
        })
    }
}

impl TryFrom<&Arc<InFileChunk>> for StoredChunkRef {
    type Error = Error;
    fn try_from(value: &Arc<InFileChunk>) -> Result<Self, Self::Error> {
        Self::try_from((**value).clone())
    }
}

impl TryFrom<&InFileChunk> for StoredChunkRef {
    type Error = Error;
    fn try_from(value: &InFileChunk) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

impl TryFrom<&mut InFileChunk> for StoredChunkRef {
    type Error = Error;
    fn try_from(value: &mut InFileChunk) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

impl InFileChunk {
    /// Write a chunk to the file at all the registered paths for that chunk
    pub fn write(&self, hash: &Hash, chunk: &[u8]) -> Result<(), Error> {
        tracing::debug!(
            "Writing {hash}, {} bytes at {} locations",
            chunk.len(),
            self.paths.len()
        );
        assert_eq!(&self.info.hash, hash);

        if self.populated.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!("Already populated {hash}, skipping");
            return Ok(());
        }

        let mut count = 0;
        // write chunk to all associated files (and offsets)
        self.paths
            .iter()
            .try_for_each(|p| {
                tracing::trace!("Writing InFileChunk for {hash} @ {:?}", p);
                // TODO this doesn't work with File::create, I'm not sure why
                let mut buffer = File::options()
                    .write(true)
                    .open(&p.path)
                    .inspect_err(|e| tracing::error!("Cannot create file at {:?}: {}", p.path, e))
                    .map_err(Error::msg)?;
                buffer.seek(std::io::SeekFrom::Start(p.offset)).unwrap();
                match buffer.write(chunk) {
                    Ok(size) if size as u32 == self.info.size => Ok(()),
                    Ok(size) => Err(Error::msg(format!(
                        "Wrote {size} instead of expected {}",
                        self.info.size
                    ))),
                    Err(e) => Err(Error::msg(format!("Cannot write to file: {e}"))),
                }
                .map(|()| {
                    self.populated
                        .swap(true, std::sync::atomic::Ordering::Relaxed);
                    // TODO investigate:
                    // I noticed while testing that even calling sync_all doesn't ensure the write actually happens
                    // right away. I'm not sure why this is the case.
                    //buffer.sync_all().unwrap();
                })
                .inspect(|_| count += chunk.len())
            })
            .inspect(|_| tracing::trace!("{count} bytes written"))
            .inspect_err(|e| tracing::error!("Failed writing {hash} after {count} bytes: {e}"))
    }
}

/// Shared state pointed to by `FsStorage`
/// This does Chunks ref-counting on Items to manage chunks availability trought `FsStorage` `ChunkStorage` interfaces.
/// Removing files may actually be slow.
#[derive(Default)]
struct InnerFsStorage {
    /// Items, used to get the paths where to store chunks
    /// Keeping track of all items'paths is important, as we cannot store different items in the same path
    pub root: PathBuf,

    /// Items, used to get the paths where to store chunks
    pub items: HashSet<Item>,

    /// Data, used to store `InFileChunks` (stored nodes) and link nodes
    pub data: HashMap<Hash, InFileChunk>,
    pub links: HashMap<Hash, Arc<StoredChunkRef>>,
}

impl InnerFsStorage {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            ..Default::default()
        }
    }
}

#[cfg(target_os = "linux")]
fn preallocate_file(path: &Path, len: usize) {
    use libc;
    let file = File::create(path).unwrap();
    let res = unsafe {
        let res = libc::fallocate(file.as_raw_fd(), 0, 0, len as i64);
        libc::sync();
        res
    };
    tracing::debug!("`fallocate` returned {res}")
}

#[cfg(not(target_os = "linux"))]
fn preallocate_file(path: &Path, len: usize) {
    tracing::trace!("preallocate_file: {len} bytes at {path:?}");
    let mut file = File::create(path)
        .inspect_err(|e| tracing::error!("Cannot create file at {:?}: {}", path, e))
        .unwrap();
    file.set_len(len as u64).unwrap();
    file.sync_all().unwrap();
}

impl InnerFsStorage {
    /// Returns the (eventual) stored path of the item provided
    fn path(&self, path: &Path) -> PathBuf {
        // TODO also create parent?
        if path.starts_with(&self.root) {
            path.to_path_buf()
        } else {
            crate::utils::path::join(&self.root, path)
        }
    }

    /// Returns the (eventual) stored path of the item provided
    fn item_path(&self, item: &Item) -> Result<PathBuf, Error> {
        let full_path = self.root.join(
            item.metadata
                .path
                .strip_prefix("/")
                .unwrap_or(&item.metadata.path),
        );
        create_dir_all(full_path.parent().unwrap_or(&full_path)).map_err(Error::msg)?;
        tracing::debug!(
            "Created path {:?}",
            full_path.parent().unwrap_or(&full_path)
        );
        Ok(full_path)
    }

    /// Pre-allocate space for multiple `ChunkInfo` in the filesystem at a path
    pub fn pre_allocate(&mut self, path: &Path, data: &[ChunkInfo]) -> Result<(), Error> {
        tracing::debug!(
            "Preallocating {} chunks at {path:?}, for a total of {} bytes",
            data.len(),
            data.iter().map(|x| x.size as u64).sum::<u64>()
        );
        if self
            .items
            .iter()
            .any(|it| self.item_path(it).is_ok_and(|p| p == path))
        {
            return Err(Error::msg(format!(
                "Conflict found: path {} already present in filesystem",
                path.to_string_lossy()
            )));
        }

        preallocate_file(&self.path(path), data.iter().map(|x| x.size as usize).sum());

        let mut offset = 0;

        // Prepare all InFileChunk and add them to self.data
        for chunk in data {
            let paths = InFileChunkPaths {
                path: self.path(path),
                offset,
            };
            offset += chunk.size as u64;
            if let Some(infile_chunk) = self.data.get_mut(&chunk.hash) {
                infile_chunk.paths.insert(paths);
            } else {
                self.data.insert(
                    chunk.hash,
                    InFileChunk {
                        info: *chunk,
                        paths: HashSet::from_iter([paths]),
                        populated: Arc::default(),
                    },
                );
            }
        }
        Ok(())
    }

    /// Pre-allocate space for `Bytes` in the filesystem at a path
    pub fn pre_allocate_bytes(&mut self, path: &Path, data: &[u8]) -> Result<(), Error> {
        tracing::debug!("Preallocating {} bytes at {path:?}", data.len());
        let chunks: Vec<ChunkInfo> = data
            .chunks(CHUNK_SIZE)
            .into_iter()
            .map(|x| ChunkInfo {
                hash: do_hash(x),
                size: x.len() as u32,
            })
            .collect();
        self.pre_allocate(path, &chunks.as_slice())
    }

    /// Pre-allocate space for an item in the filesystem
    pub fn pre_allocate_item(&mut self, item: &Item) -> Result<(), Error> {
        let path = self.item_path(item)?;
        tracing::debug!("Preallocating item to {:?}", path);
        if self.items.contains(item) {
            return Ok(());
        };

        self.pre_allocate(&path, &item.chunks[..])?;

        self.items.insert(item.clone());
        Ok(())
    }

    /// Remove references to file from `FsStorage`, doesn't actually delete the file from filesystem
    pub fn remove(&mut self, item: Item) -> Result<(), Error> {
        let path = self.item_path(&item)?;
        // First check wheter the item is actually present, if not return Err
        let item = self
            .items
            .remove(&item)
            .then_some(item)
            .ok_or(Error::msg("Item was not present in FsStorage"))?;

        for chunk in &item.chunks {
            // We may have duplicated hashes in chunks, so we need to check first if it's there and otherwise
            // do nothing (may not be there, or we may have already removed it). HashMap::get/get_mut do it for us.
            if let Some(infile_chunk) = self.data.get_mut(&chunk.hash) {
                // Remove any chunk referencing the item's path
                let _ = infile_chunk.paths.extract_if(|infile| infile.path == path);
                // If there are not references left, outright remove the entry
                if infile_chunk.paths.is_empty() {
                    self.data.remove(&chunk.hash);
                }
            }
            continue;
        }
        Ok(())
    }

    /// Remove references to file from `FsStorage` and deletes the file from filesystem
    fn delete(&mut self, item: Item) -> Result<(), Error> {
        let path = self.item_path(&item)?;
        self.remove(item)
            .and_then(|()| remove_file(path).map_err(Error::msg))
    }
}

/// Storage keeping files in the filesystem instead of stored chunks indipendently
///
/// It is useful to actually install files in the filesystem if the root is set to `/`
///
/// While it implements `ChunkStorage`, most methods will fail without special care, in particular by providing
/// relevant items to get their paths.
///
/// Most logic is implemented in `InnerFsStorage`, this is mostly a wrapper to provide interior mutability
#[derive(Clone, Default)]
pub struct FsStorage {
    /// Reference to data and some auxiliary maps to keep track of different chunks and paths
    data: Arc<RwLock<InnerFsStorage>>,
}

impl FsStorage {
    pub fn new(root: PathBuf) -> Result<Self, std::io::Error> {
        create_dir_all(&root)?;
        Ok(Self {
            data: Arc::new(RwLock::new(InnerFsStorage::new(root))),
        })
    }

    //pub fn load() {}

    /// Call `read()` on self.data lock and unwrap it
    fn read(&self) -> std::sync::RwLockReadGuard<InnerFsStorage> {
        self.data.read().expect("Poisoned Lock")
    }

    /// Call `write()` on self.data lock and unwrap it
    fn write(&self) -> std::sync::RwLockWriteGuard<InnerFsStorage> {
        self.data.write().expect("Poisoned Lock")
    }

    /// Root path for the `FsStorage`
    #[must_use]
    pub fn root(&self) -> PathBuf {
        self.read().root.clone()
    }

    #[must_use]
    pub fn items(&self) -> HashSet<Item> {
        self.read().items.clone()
    }

    pub fn path(&self, path: &Path) -> PathBuf {
        self.read().path(path)
    }

    /// Wrapper around `InnerFsStorage::pre_allocate`
    pub fn pre_allocate(&self, path: &Path, data: &[ChunkInfo]) -> Result<(), Error> {
        self.write().pre_allocate(&path, data)
    }

    /// Wrapper around `InnerFsStorage::pre_allocate_bytes`
    pub fn pre_allocate_bytes(&self, path: &Path, data: &[u8]) -> Result<(), Error> {
        self.write().pre_allocate_bytes(&path, data)
    }

    /// Wrapper around `InnerFsStorage::pre_allocate_item`
    pub fn pre_allocate_item(&self, item: &Item) -> Result<(), Error> {
        self.write().pre_allocate_item(item)
    }

    /// Remove references to file from `FsStorage`, doesn't actually delete the file from filesystem
    /// Wrapper around `InnerFsStorage::remove`
    pub fn remove(&self, item: Item) -> Result<(), Error> {
        self.write().remove(item)
    }

    /// Remove references to file from `FsStorage` and deletes the file from filesystem
    /// Wrapper around `InnerFsStorage::delete`
    pub fn delete(&self, item: Item) -> Result<(), Error> {
        self.write().delete(item)
    }
}

impl ChunkStorage for FsStorage {
    /// Get a `StoredChunkRef` chunk from storage
    fn get(&self, hash: &Hash) -> Option<Arc<StoredChunkRef>> {
        let inner = self.read();
        inner.links.get(hash).cloned().or(inner
            .data
            .get(hash)
            .and_then(|x| StoredChunkRef::try_from(x).ok())
            .map(Arc::new))
    }

    fn size(&self) -> usize {
        0 // TODO
    }

    /// May only add chunks by adding items. Dummy implementation always returning None
    fn _insert_chunk(&self, hash: Hash, chunk: &[u8]) -> Option<Arc<StoredChunkRef>> {
        tracing::trace!("Insert chunk {hash}, {} bytes", chunk.len());

        self.write().data.get_mut(&hash).and_then(|infile_chunk| {
            infile_chunk
                .write(&hash, chunk)
                .inspect_err(|e| tracing::error!("Cannot write infile chunk: {e}"))
                .ok()

            /*
                StoredChunkRef::try_from(infile_chunk)
                    .inspect_err(|e| tracing::error!("Cannot create StoredChunkRef: {e}"))
                    .map(Arc::new)
                    .inspect_err(|e| tracing::error!("Cannot insert chunk: {e}"))
                    .ok()
            */
        });
        Some(Arc::new(StoredChunkRef::Stored {
            hash,
            data: Arc::new(chunk.to_vec()),
        }))
    }

    /// Create a new Item from its metadata and Bytes
    /// This is the preferred way to create a new Item
    fn create_item(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        file: bytes::Bytes,
    ) -> Option<Item>
    where
        Self: Sized,
    {
        tracing::debug!("Create item {name} at {path:?}");
        self.pre_allocate_bytes(&path, &file).ok()?;
        tracing::info!("Preallocated on disk {:?}", path);

        let hash_tree = self.insert(file)?;
        let item = Item::new(name, path, revision, description, hash_tree);
        tracing::debug!("Inserted chunks in storage for item {}", item.metadata.name);
        tracing::trace!("New item: {item:?}");

        Some(item)
    }

    /// May only link chunks by adding items. Dummy implementation always returning None
    fn _link(
        &self,
        hash: Hash,
        left: Arc<StoredChunkRef>,
        right: Arc<StoredChunkRef>,
    ) -> Option<Arc<StoredChunkRef>> {
        let mut l = self.write();
        let res = l
            .links
            .try_insert(hash, Arc::new(StoredChunkRef::Parent { hash, left, right }));
        Some(res.map_or_else(|e| (*e.entry.get()).clone(), |x| (*x).clone()))
    }

    /// Get a Vec of all chunks' hashes in storage
    fn chunks(&self) -> Vec<Hash> {
        self.read().data.keys().copied().collect()
    }

    /*
    fn insert(&self, data: bytes::Bytes) -> Option<Arc<StoredChunkRef>>
        where
            Self: Sized, {

    }
    */
}

#[cfg(test)]
mod tests {
    use crate::{
        chunks::CHUNK_SIZE,
        hash::hash as do_hash,
        item::tests::{make_ones_item, new_dummy_item, random_path},
    };
    use std::{str::FromStr, thread::sleep, time::Duration};

    use test_log::test;

    use super::*;

    fn print_fsstorage(storage: &FsStorage) {
        println!(
            "root: {:?} \n\
            chunks: {:?} \n\
            file chunks: {:?} \n\
            items: {:?}",
            storage.root(),
            storage.chunks(),
            storage.data.read().unwrap().data.values(),
            storage.items(),
        );
    }

    fn make_infile_chunk<const SIZE: usize>() -> ([u8; SIZE], Hash, InFileChunk) {
        // Create infile_chunk with some data
        let data = [1u8; SIZE];
        let hash = do_hash(&data);
        let infile_chunk = InFileChunk {
            info: ChunkInfo {
                hash,
                size: SIZE as u32,
            },
            paths: HashSet::default(),
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
        infile_chunk.paths.insert(InFileChunkPaths {
            path: path.clone(),
            offset: 0,
        });

        // write to temp path
        infile_chunk.write(&hash, data).unwrap();

        // wait for the file to do actually written. We're calling `sync_all` inside InFileChunk::write,
        // maybe I'm missing something.
        sleep(Duration::from_secs(2));

        path
    }

    fn is_populated(infile_chunk: &InFileChunk) -> bool {
        infile_chunk
            .populated
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Creates a random path in /tmp, ensuring its parent directory exists
    fn temp_path() -> PathBuf {
        let path = std::env::temp_dir().join(random_path());
        create_dir_all(path.parent().unwrap_or(&path)).unwrap();
        path
    }

    #[test]
    fn test_preallocate_file() {
        let path = temp_path();
        println!("{path:?}");
        preallocate_file(&path, 22);
        assert!(path.exists());
        let mut buf = vec![];
        File::open(path).unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf.len(), 22);
    }

    #[test]
    fn test_infile_chunk() {
        let (data, hash, mut infile_chunk) = make_infile_chunk::<CHUNK_SIZE>();
        assert!(!is_populated(&infile_chunk));

        let path = write_data_to_infile_chunk(&data, hash, &mut infile_chunk);
        assert!(is_populated(&infile_chunk));

        // Check written data
        let mut f = File::open(&path).unwrap();
        let mut buffer = [0u8; CHUNK_SIZE];
        let n = f.read(&mut buffer[..]).unwrap();
        assert_eq!(n, CHUNK_SIZE);
        assert_eq!(do_hash(&buffer), hash);
    }

    #[test]
    fn test_infile_chunk_conversion() {
        let (data, hash, mut infile_chunk) = make_infile_chunk::<CHUNK_SIZE>();
        let _path = write_data_to_infile_chunk(&data, hash, &mut infile_chunk);

        assert!(is_populated(&infile_chunk));

        let chunk = StoredChunkRef::try_from(infile_chunk.clone()).unwrap();
        assert_eq!(do_hash(&chunk.stored_data().unwrap()), do_hash(&data));
        assert_eq!(chunk.hash(), &hash);
        assert_eq!(do_hash(&data), hash);

        // Check idempotence
        let chunk2 = StoredChunkRef::try_from(infile_chunk).unwrap();
        assert_eq!(chunk, chunk2);
    }

    #[test]
    fn test_fs_storage() {
        // check default doesn't panics, just in case
        let storage = FsStorage::default();
        print_fsstorage(&storage);

        // create storage in a temporary directory
        let tempdir = std::env::temp_dir().join(PathBuf::from_str("in_a_temp_dir").unwrap());
        let storage = FsStorage::new(tempdir.clone()).unwrap();

        // make an item with a know content, a single chunk of all zeros
        let item = make_ones_item();
        storage.pre_allocate_item(&item).unwrap();

        let itempath = crate::utils::path::join(&tempdir, &item.metadata.path);
        assert!(itempath.exists());

        // Actually store the item chunk
        storage.insert_chunk(&[1u8; CHUNK_SIZE]).unwrap();

        print_fsstorage(&storage);

        let path = storage.data.read().unwrap().item_path(&item).unwrap();
        let mut f = File::open(&path).unwrap();
        let mut buffer = vec![0u8; item.size() as usize];

        println!("Stored data path {path:?}");

        // read from file
        let n = f.read(&mut buffer[..]).unwrap();

        assert_eq!(n, item.size() as usize);
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
    fn test_fs_storage_round_trip() {
        // create storage in a temporary directory
        let tempdir = std::env::temp_dir().join(PathBuf::from_str("in_a_temp_dir").unwrap());
        let storage = FsStorage::new(tempdir.clone()).unwrap();

        let item = new_dummy_item::<FsStorage, 1u8, 1_000_000>(storage.clone());
        println!("Created item: {item:?}");
        print_fsstorage(&storage);

        let stored = storage
            .get(&item.metadata.root.hash)
            .unwrap()
            .clone_data()
            .unwrap();

        assert_eq!(stored.len(), 1_000_000);
        for b in stored {
            assert_eq!(b, 1u8);
        }
    }
}
