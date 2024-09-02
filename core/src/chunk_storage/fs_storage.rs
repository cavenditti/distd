use std::{
    collections::{HashMap, HashSet},
    fs::{create_dir_all, remove_file, File},
    io::{BufReader, Read, Seek},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
};

use anyhow::Error;
use blake3::Hash;

use crate::{chunks::ChunkInfo, item::Item};

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
    buf_reader: Arc<Mutex<Option<BufReader<File>>>>,
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
        let mut buf_reader = value.buf_reader.lock().expect("Poisoned Lock");

        let first_path = value.paths.iter().next().ok_or(Error::msg("empty"))?;
        if buf_reader.is_none() {
            let file = File::open(&first_path.path).map_err(Error::msg)?;
            let reader = BufReader::new(file);
            *buf_reader = Some(reader);
        }
        let reader = buf_reader
            .as_mut()
            .ok_or(Error::msg("Cannot open buffer"))?;
        reader
            .seek(std::io::SeekFrom::Start(first_path.offset))
            .map_err(Error::msg)?;

        let mut buf = vec![0u8; value.info.size as usize];

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
        assert_eq!(&self.info.hash, hash);

        // write chunk to all associated files (and offsets)
        self.paths.iter().try_for_each(|p| {
            let buffer = File::create(&p.path).map_err(Error::msg)?;
            match buffer.write_at(chunk, p.offset) {
                Ok(size) if size as u32 == self.info.size => Ok(()),
                _ => Err(Error::msg("Cannot write to file")),
            }
            .map(|()| {
                self.populated
                    .swap(true, std::sync::atomic::Ordering::Relaxed);
                // TODO investigate:
                // I noticed while testing that even calling sync_all doesn't ensure the write actually happens
                // right away. I'm not sure why this is the case.
                buffer.sync_all().unwrap();
            })
        })
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

    /// Data, used to store `InFileChunks`
    pub data: HashMap<Hash, InFileChunk>,
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
    fn path(&self, item: &Item) -> Result<PathBuf, Error> {
        let full_path = self.root.join(
            item.metadata
                .path
                .strip_prefix("/")
                .unwrap_or(&item.metadata.path),
        );
        create_dir_all(full_path.parent().unwrap_or(&full_path)).map_err(Error::msg)?;
        Ok(full_path)
    }

    /// Pre-allocate space for an item in the filesystem
    pub fn pre_allocate(&mut self, item: &Item) -> Result<(), Error> {
        let path = self.path(item)?;
        if self.items.contains(item) {
            return Ok(());
        };
        if self
            .items
            .iter()
            .any(|it| self.path(it).is_ok_and(|p| p == path))
        {
            return Err(Error::msg(format!(
                "Conflict found: path {} already present in filesystem",
                path.to_string_lossy()
            )));
        }
        File::create(&path)
            .and_then(|x| x.set_len(u64::from(item.size())))
            .map_err(Error::msg)?;

        let mut offset = 0;

        // Prepare all InFileChunk and add them to self.data
        for chunk in &item.chunks {
            let paths = InFileChunkPaths {
                path: path.clone(),
                offset,
            };
            offset += u64::from(chunk.size);
            if let Some(infile_chunk) = self.data.get_mut(&chunk.hash) {
                infile_chunk.paths.insert(paths);
            } else {
                self.data.insert(
                    chunk.hash,
                    InFileChunk {
                        info: *chunk,
                        paths: HashSet::from_iter([paths]),
                        populated: Arc::default(),
                        buf_reader: Arc::default(),
                    },
                );
            }
        }
        self.items.insert(item.clone());
        Ok(())
    }

    /// Remove references to file from `FsStorage`, doesn't actually delete the file from filesystem
    pub fn remove(&mut self, item: Item) -> Result<(), Error> {
        let path = self.path(&item)?;
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
        let path = self.path(&item)?;
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
    #[must_use] pub fn root(&self) -> PathBuf {
        self.read().root.clone()
    }

    #[must_use] pub fn items(&self) -> HashSet<Item> {
        self.read().items.clone()
    }

    /// Wrapper around `InnerFsStorage::pre_allocate`
    pub fn pre_allocate(&self, item: &Item) -> Result<(), Error> {
        self.write().pre_allocate(item)
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
        self.read()
            .data
            .get(hash)
            .and_then(|x| StoredChunkRef::try_from(x).ok())
            .map(Arc::new)
    }

    fn size(&self) -> usize {
        0 // TODO
    }

    /// May only add chunks by adding items. Dummy implementation always returning None
    fn _insert_chunk(&self, hash: Hash, chunk: &[u8]) -> Option<Arc<StoredChunkRef>> {
        self.write().data.get_mut(&hash).and_then(|infile_chunk| {
            infile_chunk.write(&hash, chunk).ok()?;
            StoredChunkRef::try_from(infile_chunk).map(Arc::new).ok()
        })
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

    /// Get a Vec of all chunks' hashes in storage
    fn chunks(&self) -> Vec<Hash> {
        self.read()
            .data
            .keys()
            .copied()
            .collect()
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
    use crate::{chunks::CHUNK_SIZE, hash::hash as do_hash, item::tests::make_ones_item};
    use std::{str::FromStr, thread::sleep, time::Duration};

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

    fn make_infile_chunk() -> ([u8; CHUNK_SIZE], Hash, InFileChunk) {
        // Create infile_chunk with some data
        let data = [1u8; CHUNK_SIZE];
        let hash = do_hash(&data);
        let infile_chunk = InFileChunk {
            info: ChunkInfo {
                hash,
                size: CHUNK_SIZE as u32,
            },
            paths: HashSet::default(),
            populated: Arc::default(),
            buf_reader: Arc::default(),
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

    #[test]
    fn test_infile_chunk() {
        let (data, hash, mut infile_chunk) = make_infile_chunk();
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
        let (data, hash, mut infile_chunk) = make_infile_chunk();
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
        let storage = FsStorage::new(tempdir).unwrap();

        // make an item with a know content, a single chunk of all zeros
        let item = make_ones_item();
        storage.pre_allocate(&item).unwrap();

        // Actually store the item chunk
        storage.insert_chunk(&[1u8; CHUNK_SIZE]).unwrap();

        print_fsstorage(&storage);

        // TODO read file and check its content are correct
        let path = storage.data.read().unwrap().path(&item).unwrap();
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

        // TODO do the same with an item with multiple chunks
    }
}
