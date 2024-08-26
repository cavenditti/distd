use std::{
    collections::{HashMap, HashSet},
    fs::{remove_file, File},
    io::{BufReader, Read, Seek},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
};

use anyhow::Error;
use blake3::Hash;

use crate::{chunks::ChunkInfo, item::Item};

use super::{ChunkStorage, StoredChunkRef};

#[derive(Debug, Clone)]
struct InFileChunk {
    pub info: ChunkInfo,

    pub path: PathBuf,
    pub offset: u64,
    pub populated: Arc<AtomicBool>,
    buf_reader: Arc<Mutex<Option<BufReader<File>>>>,
}

impl TryFrom<InFileChunk> for StoredChunkRef {
    type Error = Error;

    fn try_from(value: InFileChunk) -> Result<Self, Self::Error> {
        if !value.populated.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::msg("missing data"));
        }
        let mut buf_reader = value
            .buf_reader
            .lock()
            .map_err(|_| Error::msg("Cannot acquire lock"))?;
        if buf_reader.is_none() {
            let file = File::open(value.path).map_err(Error::msg)?;
            let reader = BufReader::new(file);
            *buf_reader = Some(reader);
        }
        let reader = buf_reader.as_mut().unwrap();
        reader
            .seek(std::io::SeekFrom::Start(value.offset))
            .map_err(Error::msg)?;
        let mut buf = Vec::with_capacity(value.info.size as usize);
        reader.read_exact(&mut buf).map_err(Error::msg)?;
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

impl InFileChunk {
    pub fn write(&self, hash: &Hash, chunk: &[u8]) -> Result<(), Error> {
        assert_eq!(&self.info.hash, hash);

        let buffer = File::create("foo.txt").map_err(Error::msg)?;
        match buffer.write_at(chunk, self.offset) {
            Ok(size) if size as u32 == self.info.size => Ok(()),
            _ => Err(Error::msg("Cannot write to file")),
        }
        .map(|_| {
            self.populated
                .swap(true, std::sync::atomic::Ordering::Relaxed);
        })
    }
}

/// Shared state pointed to by FsStorage
/// This does Chunks ref-counting on Items to manage chunks availability trought FsStorage ChunkStorage interfaces.
/// Removing files may actually be slow.
#[derive(Default)]
struct InnerFsStorage {
    /// Items, used to get the paths where to store chunks
    /// Keeping track of all items'paths is important, as we cannot store different items in the same path
    pub root: PathBuf,

    pub items: HashSet<Item>,
    pub data: HashMap<Hash, Vec<Arc<InFileChunk>>>,
}

impl InnerFsStorage {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            ..Default::default()
        }
    }
}

impl InnerFsStorage {
    /// Returns the (eventual) stored path of the item provided
    fn path(&self, item: &Item) -> PathBuf {
        self.root.join(&item.metadata.path)
    }

    pub fn get(&self, hash: &Hash) -> Option<Arc<StoredChunkRef>> {
        self.data
            .get(hash)
            .and_then(|x| StoredChunkRef::try_from(x.first()?).ok())
            .map(Arc::new)
    }

    pub fn chunks(&self) -> Vec<Hash> {
        self.data.keys().cloned().collect()
    }

    pub fn pre_allocate(&mut self, item: Item) -> Result<(), Error> {
        let path = self.path(&item);
        if self.items.contains(&item) {
            return Ok(());
        };
        if self.items.iter().any(|it| *self.path(it) == path) {
            return Err(Error::msg(format!(
                "Conflict found: path {} already present in filesystem",
                path.to_string_lossy()
            )));
        }
        File::create(&path)
            .and_then(|x| x.set_len(item.get_size() as u64))
            .map_err(Error::msg)?;

        let mut offset = 0;

        // Prepare all InFileChunk and add them to self.data
        for chunk in &item.chunks {
            let infile_chunk = InFileChunk {
                info: *chunk,
                path: path.clone(),
                offset,
                populated: Arc::default(),
                buf_reader: Arc::default(),
            };
            offset += chunk.size as u64;
            if let Some(v) = self.data.get_mut(&chunk.hash) {
                v.append(infile_chunk);
            } else {
                self.data.insert(&chunk.hash, infile_chunk)
            }
        }
        self.items.insert(item);
        Ok(())
    }

    /// Remove references to file from FsStorage, doesn't actually delete the file from filesystem
    pub fn remove(&mut self, item: Item) -> Result<(), Error> {
        let path = self.path(&item);
        // First check wheter the item is actually present, if not return Err
        let item = self
            .items
            .remove(&item)
            .then_some(item)
            .ok_or(Error::msg("Item was not present in FsStorage"))?;

        for chunk in &item.chunks {
            // We may have duplicated hashes in chunks, so we need to check first if it's there and otherwise
            // do nothing (may not be there, or we may have already removed it). HashMap::get/get_mut do it for us.
            if let Some(v) = self.data.get_mut(&chunk.hash) {
                // Remove any chunk referencing the item's path
                let _ = v.extract_if(|infile| infile.path == path);
                // If there are not references left, outright remove the entry
                if v.is_empty() {
                    self.data.remove(&chunk.hash);
                }
            }
            continue;
        }
        Ok(())
    }

    /// Remove references to file from FsStorage and deletes the file from filesystem
    fn delete(&mut self, item: Item) -> Result<(), Error> {
        let path = self.path(&item);
        self.remove(item)
            .and_then(|_| remove_file(path).map_err(Error::msg))
    }
}

/// Storage keeping files in the filesystem instead of stored chunks indipendently
///
/// It is useful to actually install files in the filesystem if the root is set to `/`
///
/// While it implements ChunkStorage, most methods will fail without special care, in particular by providing
/// relevant items to get their paths.
///
/// Most logic is implemented in `InnerFsStorage`, this is mostly a wrapper to provide interior mutability
#[derive(Clone, Default)]
pub struct FsStorage {
    /// Reference to data and some auxiliary maps to keep track of different chunks and paths
    data: Arc<RwLock<InnerFsStorage>>,
}

impl FsStorage {
    pub fn new(root: PathBuf) -> Self {
        Self {
            data: Arc::new(RwLock::new(InnerFsStorage::new(root))),
        }
    }

    //pub fn load() {}

    /// Wrapper around InnerFsStorage::pre_allocate
    pub fn pre_allocate(&self, item: Item) -> Result<(), Error> {
        self.data
            .write()
            .map_err(|_| Error::msg("cannot acquire lock"))?
            .pre_allocate(item)
    }

    /// Remove references to file from FsStorage, doesn't actually delete the file from filesystem
    /// Wrapper around InnerFsStorage::remove
    pub fn remove(&self, item: Item) -> Result<(), Error> {
        self.data
            .write()
            .map_err(|_| Error::msg("cannot acquire lock"))?
            .remove(item)
    }

    /// Remove references to file from FsStorage and deletes the file from filesystem
    /// Wrapper around InnerFsStorage::delete
    pub fn delete(&self, item: Item) -> Result<(), Error> {
        self.data
            .write()
            .map_err(|_| Error::msg("cannot acquire lock"))?
            .delete(item)
    }
}

impl ChunkStorage for FsStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<StoredChunkRef>> {
        self.data.read().ok()?.get(hash)
    }

    fn size(&self) -> usize {
        0 // TODO
    }

    /// May only add chunks by adding items. Dummy implementation always returning None
    fn _insert_chunk(&self, hash: Hash, chunk: &[u8]) -> Option<Arc<StoredChunkRef>> {
        let mut inner = self.data.write().ok()?;
        if let Some(v) = inner.data.get_mut(&hash) {
            v.iter_mut().map(|x| x.write(&hash, chunk));
        }
        None
    }

    /// May only link chunks by adding items. Dummy implementation always returning None
    fn _link(
        &self,
        _hash: Hash,
        _left: Arc<StoredChunkRef>,
        _right: Arc<StoredChunkRef>,
    ) -> Option<Arc<StoredChunkRef>> {
        None
    }

    fn chunks(&self) -> Vec<Hash> {
        self.data.read().unwrap().chunks()
    }
}
