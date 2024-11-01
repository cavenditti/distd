use std::{
    collections::{HashMap, HashSet},
    fs::{create_dir_all, remove_file, File},
    io::{BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

use tokio_stream::{Stream, StreamExt};

use crate::{
    chunk_storage::StorageError,
    chunks::{ChunkInfo, CHUNK_SIZE},
    error::Error,
    hash::{hash as do_hash, Hash, HashTreeCapable},
    item::{Item, Name as ItemName},
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
}

impl Handle {
    pub fn new(path: &Path) -> Result<Self, Error> {
        Ok(Self {
            buf_writer: BufWriter::with_capacity(CHUNK_SIZE * 8, open_file(path)?),
        })
    }

    pub fn write(&mut self, chunk: &[u8], offset: u64) -> Result<(), Error> {
        self.buf_writer.seek(std::io::SeekFrom::Start(offset))?;
        self.buf_writer
            .write_all(chunk)
            .map_err(Error::IoError)
            .and(self.buf_writer.flush().map_err(Error::IoError))
    }
}

/// Chunk stored in multiple files
/// Basically ref-counting on items paths
#[derive(Debug, Clone)]
struct InFileChunk {
    pub info: ChunkInfo,
    pub path: PathBuf,
    pub offset: u64,
    pub populated: Arc<AtomicBool>,
    //pub cached: Arc<Mutex<Option<Bytes>>>,
    //buf_reader: Arc<Mutex<Option<BufReader<loadFile>>>>,
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

        let mut buf = vec![0u8; usize::try_from(value.info.size)?];

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
            "Writing {hash}, {} bytes at {}",
            chunk.len(),
            self.path.to_string_lossy()
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

/// Storage keeping files in the filesystem instead of stored chunks indipendently
///
/// It is useful to actually install files in the filesystem if the root is set to `/`
///
/// While it implements `ChunkStorage`, most methods will fail without special care, in particular by providing
/// relevant items to get their paths.
///
/// Most logic is implemented in `InnerFsStorage`, this is mostly a wrapper to provide interior mutability
#[derive(Default)]
pub struct FsStorage {
    /// Items, used to get the paths where to store chunks
    /// Keeping track of all items'paths is important, as we cannot store different items in the same path
    pub root: PathBuf,

    /// Items, used to get the paths where to store chunks
    pub items: HashSet<Item>,

    /// Data, used to store `InFileChunks` (stored nodes) and link nodes
    data: HashMap<Hash, InFileChunk>,
    links: HashMap<Hash, Arc<Node>>,

    handles_map: HashMap<PathBuf, Handle>,
}

impl FsStorage {
    #[must_use] pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            ..Default::default()
        }
    }

    /// Returns the (eventual) stored path of the item provided
    #[must_use] pub fn path(&self, path: &Path) -> PathBuf {
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
        create_dir_all(full_path.parent().unwrap_or(&full_path))?;
        tracing::debug!(
            "Created path {:?}",
            full_path.parent().unwrap_or(&full_path)
        );
        Ok(full_path)
    }

    /// Pre-allocate a single `ChunkInfo` in the filesystem at a path
    pub fn pre_allocate_chunk(
        &mut self,
        path: &Path,
        chunk_info: &ChunkInfo,
        offset: u64,
    ) -> Result<(), Error> {
        // If already exists do nothing
        if self.data.contains_key(&chunk_info.hash) {
            return Ok(());
        }

        let ifc = InFileChunk {
            info: *chunk_info,
            path: path.to_owned(),
            offset,
            populated: Arc::default(),
        };
        self.data.insert(chunk_info.hash, ifc);
        if !self.handles_map.contains_key(path) {
            self.handles_map.insert(path.to_owned(), Handle::new(path)?);
        }
        Ok(())
    }

    /// Pre-allocate space for multiple `ChunkInfo` in the filesystem at a path
    pub fn pre_allocate(&mut self, path: &Path, data: &[ChunkInfo]) -> Result<(), Error> {
        tracing::debug!(
            "Preallocating {} chunks at {path:?}, for a total of {} bytes",
            data.len(),
            data.iter().map(|x| x.size).sum::<u64>()
        );
        tracing::trace!(
            "Preallocating [{}] at {path:?}",
            data.iter()
                .map(|chunk| chunk.hash)
                .map(|h| h.to_string() + ", ")
                .collect::<String>(),
        );

        let mut offset = 0;

        // Prepare all InFileChunk and add them to self.data
        for chunk in data {
            self.pre_allocate_chunk(path, chunk, offset)?;
            offset += chunk.size;
        }
        Ok(())
    }

    /// Pre-allocate space for `Bytes` in the filesystem at a path
    pub fn pre_allocate_bytes(&mut self, path: &Path, data: &[u8]) -> Result<(), Error> {
        tracing::debug!("Preallocating {} bytes at {path:?}", data.len());
        let chunks = data
            .chunks(CHUNK_SIZE)
            .map(|chunk| ChunkInfo {
                hash: do_hash(chunk),
                size: chunk.len() as u64,
            })
            .collect::<Vec<ChunkInfo>>();

        self.pre_allocate(path, &chunks)
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
            .ok_or(Error::MissingData)?;

        for chunk in &item.chunks {
            // We may have duplicated hashes in chunks, so we need to check first if it's there and otherwise
            // do nothing (may not be there, or we may have already removed it). HashMap::get/get_mut do it for us.
            if let Some(infile_chunk) = self.data.get_mut(&chunk.hash) {
                if infile_chunk.path == path {
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
            .and_then(|()| remove_file(path).map_err(Error::IoError))
    }
}

impl ChunkStorage for FsStorage {
    /// Get a `Node` chunk from storage
    fn get(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.links.get(hash).cloned().or(self
            .data
            .get(hash)
            .and_then(|x| Node::try_from(x).ok())
            .map(Arc::new))
    }

    fn size(&self) -> u64 {
        0 // TODO
    }

    /// Insert chunk into storage, requires an item to have been created with the appropriate chunks to be preallocate
    fn _insert_chunk(&mut self, hash: Hash, chunk: &[u8]) -> Option<Arc<Node>> {
        self.data
            .get_mut(&hash)
            .and_then(|infile_chunk| {
                infile_chunk
                    .write(&hash, chunk, self.handles_map.get_mut(&infile_chunk.path)?)
                    .inspect_err(|e| tracing::error!("Cannot write infile chunk: {e}"))
                    .ok()

                /*
                    Node::try_from(infile_chunk)
                        .inspect_err(|e| tracing::error!("Cannot create Node: {e}"))
                        .map(Arc::new)
                        .inspect_err(|e| tracing::error!("Cannot insert chunk: {e}"))
                        .ok()
                */
            })
            .map(|()| {
                Arc::new(Node::Stored {
                    hash,
                    data: Arc::new(chunk.to_vec()),
                })
            })
    }

    /// May only link chunks by adding items. Dummy implementation always returning None
    fn _link(&mut self, hash: Hash, left: Arc<Node>, right: Arc<Node>) -> Option<Arc<Node>> {
        let size = left.size() + right.size();
        let res = self.links.try_insert(
            hash,
            Arc::new(Node::Parent {
                hash,
                left,
                right,
                size,
            }),
        );
        Some(res.map_or_else(|e| (*e.entry.get()).clone(), |x| (*x).clone()))
    }

    /// Create a new Item from its metadata and Bytes
    /// This is the preferred way to create a new Item
    fn create_item(
        &mut self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        file: bytes::Bytes,
    ) -> Option<Item>
    where
        Self: Sized,
    {
        tracing::debug!("Create item {name} with path {path:?}");
        // respect storage root
        let path = self.path(&path);
        self.pre_allocate_bytes(&path, &file).ok()?;
        tracing::info!("Preallocated on disk {:?}", path);

        let hash_tree = self.insert(file)?;
        let item = Item::new(name, path, revision, description, &hash_tree);
        tracing::debug!("New item: {item}");

        Some(item)
    }

    fn build_item(
        &mut self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        root: Arc<Node>,
    ) -> Option<Item>
    where
        Self: Sized,
    {
        tracing::debug!("Create item {name} with path {path:?}");
        // respect storage root
        let path = self.path(&path);
        self.pre_allocate(&path, &root.flatten_with_sizes()).ok()?;
        tracing::info!("Preallocated on disk {:?}", path);

        let item = Item::new(name, path, revision, description, &root);
        tracing::debug!("New item: {item}");

        Some(item)
    }

    /// Build a new Item from its metadata and a streaming of nodes
    async fn receive_item<T>(
        &mut self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        mut stream: T,
        //) -> Result<Item, crate::error::Error>
    ) -> Result<Item, crate::error::Error>
    where
        Self: Sized,
        T: Stream<Item = Node> + std::marker::Unpin,
    {
        let path = self.path(&path);

        tracing::trace!("Receiving item at '{}'", path.to_string_lossy());
        let mut i = 0; // node counter
        let mut o = 0u64; // offset counter

        // Last inserted node, will be the root at last
        let mut last: Option<Arc<Node>> = None; // final node

        while let Some(node) = stream.next().await {
            if let s_n @ Node::Stored { .. } = &node {
                tracing::trace!(
                    "Preallocating {} bytes in {}@'{}'",
                    s_n.size(),
                    o,
                    path.to_string_lossy()
                );
                self.pre_allocate_chunk(&path, &s_n.chunk_info(), o)?;
                o += s_n.size(); // FIXME this may be incorrect, should be passed along with the chunk
            }
            last = self.try_fill_in(&node);
            i += 1;
        }

        let last = last.ok_or(StorageError::TreeReconstruct)?;
        tracing::info!("Reconstructed {i} nodes with {} bytes total", last.size());

        Ok(Item::new(name, path, revision, description, &last))
    }

    /// Get a Vec of all chunks' hashes in storage
    fn chunks(&self) -> Vec<Hash> {
        self.data.keys().copied().collect()
    }

    /*
    fn insert(&self, data: bytes::Bytes) -> Option<Arc<Node>>
        where
            Self: Sized, {

    }
    */
}

impl HashTreeCapable<Arc<Node>, crate::error::Error> for FsStorage {
    fn func(&mut self, data: &[u8]) -> Result<Arc<Node>, crate::error::Error> {
        Ok(self
            .insert_chunk(data)
            .ok_or(StorageError::ChunkInsertError)?)
    }

    fn merge(&mut self, l: &Arc<Node>, r: &Arc<Node>) -> Result<Arc<Node>, crate::error::Error> {
        Ok(self
            .link(l.clone(), r.clone())
            .ok_or(StorageError::LinkCreation)?)
    }
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
            storage.root,
            storage.chunks(),
            storage.data.values(),
            storage.items,
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
    fn test_infile_chunk() {
        let (data, hash, mut infile_chunk) = make_infile_chunk::<CHUNK_SIZE>();
        println!("Created infile_chunk");
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

        let chunk = Node::try_from(infile_chunk.clone()).unwrap();
        assert_eq!(do_hash(&chunk.stored_data().unwrap()), do_hash(&data));
        assert_eq!(chunk.hash(), &hash);
        assert_eq!(do_hash(&data), hash);

        // Check idempotence
        let chunk2 = Node::try_from(infile_chunk).unwrap();
        assert_eq!(chunk, chunk2);
    }

    #[test]
    fn test_fs_storage() {
        // check default doesn't panics, just in case
        let storage = FsStorage::default();
        print_fsstorage(&storage);

        // create storage in a temporary directory
        let tempdir = std::env::temp_dir().join(PathBuf::from_str("in_a_temp_dir").unwrap());
        let mut storage = FsStorage::new(tempdir.clone());

        // make an item with a know content, a single chunk of all zeros
        let item = make_ones_item().unwrap();
        storage.pre_allocate_item(&item).unwrap();

        // Actually store the item chunk
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
        let mut storage = FsStorage::new(tempdir.clone());

        let item = new_dummy_item::<FsStorage, 1u8, 1_000_000>(&mut storage).unwrap();
        println!("Created item: {item:?}");
        print_fsstorage(&storage);

        let stored = storage.get(&item.metadata.root.hash).unwrap().clone_data();

        // reported storage size is deduplicated
        assert_eq!(stored.len(), 1_000_000);
        for b in stored {
            assert_eq!(b, 1u8);
        }
    }
}
