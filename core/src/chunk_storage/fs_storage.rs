use std::{
    collections::{HashMap, HashSet},
    fs::{self, create_dir_all, remove_file, File},
    io::{BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

use multimap::MultiMap;
use serde::{Deserialize, Serialize};
use tokio_stream::{Stream, StreamExt};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Storage keeping files in the filesystem instead of stored chunks indipendently
///
/// It is useful to actually install files in the filesystem if the root is set to `/`
///
/// While it implements `ChunkStorage`, most methods will fail without special care, in particular by providing
/// relevant items to get their paths.
///
/// Most logic is implemented in `InnerFsStorage`, this is mostly a wrapper to provide interior mutability
#[derive(Default, Serialize, Deserialize)]
pub struct FsStorage {
    /// Items, used to get the paths where to store chunks
    /// Keeping track of all items'paths is important, as we cannot store different items in the same path
    pub root: PathBuf,

    /// Items, used to get the paths where to store chunks
    pub items: HashSet<Item>,

    /// Path where to store persistent data
    persistance_path: PathBuf,

    /// Data, used to store `InFileChunks` (stored nodes) and link nodes
    data: MultiMap<Hash, InFileChunk>,
    links: HashMap<Hash, Arc<Node>>,

    #[serde(default)]
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    handles_map: HashMap<PathBuf, Handle>,
}

impl FsStorage {
    /// Create a new `FsStorage` with a root path
    ///
    /// The root path is used to store items in the filesystem
    /// The root path is not checked, it is assumed to exist and be a valid writable directory.
    ///
    /// The persistent data is stored in a well-known directory, which is created if it does not exist,
    /// as a file named after the root path, with slashes replaced by `___`
    ///
    ///
    /// # Panics
    ///
    /// Panics if the root path is not a directory
    /// Panics if the persistent data directory cannot be created
    /// Panics if the persistent data file cannot be deserialized
    /// Panics if the deserialize persistent data contains `Node::Stored` as link,
    ///     they're assumed to be all `Node::Parent`
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        // Use a well-known directory to store items info
        let persistance_dir = cache_dir().join("chunk_storage").join("fs_storage");
        let persistance_path = persistance_dir.join(root.to_string_lossy().replace('/', "___"));
        create_dir_all(persistance_dir).unwrap();

        if let Ok(file) = std::fs::read(&persistance_path) {
            // function to fill in the old links
            fn node_relink(
                s: &mut FsStorage,
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
            // deserialize storage
            let mut s: Self = bitcode::deserialize(&file).unwrap();

            let mut already_processed = HashMap::new();
            let mut old_links = s.links.clone();
            while !old_links.is_empty() {
                for n in old_links.clone().values() {
                    node_relink(&mut s, &mut already_processed, n)
                        .map(|n| old_links.remove(n.hash()));
                }
            }

            // return the recreated storage
            s
        } else {
            Self {
                root,
                persistance_path,
                ..Default::default()
            }
        }
    }

    /// Returns the (eventual) stored path of the item provided
    #[must_use]
    pub fn path(&self, path: &Path) -> PathBuf {
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

    /// Persist data to the filesystem
    fn persist(&self) -> Result<(), Error> {
        let buf = bitcode::serialize(&self)
            .inspect_err(|e| tracing::error!("{}", e))
            .map_err(InvalidParameter::from)?;
        fs::File::create(&self.persistance_path)
            .inspect_err(|e| tracing::error!("{}", e))?
            .write_all(&buf)?;
        Ok(())
    }

    /// Retrieve a Stored Node
    fn get_data(&self, hash: &Hash) -> Option<Arc<Node>> {
        // TODO implement caching here, avoid creating duplicated Node from the same InFileChunk
        // Probably requires a new Node map to be used for both links and stored, with a periodic clean up
        self.data
            .get(hash)
            .and_then(|x| Node::try_from(x).ok())
            .map(Arc::new)
    }

    /// Pre-allocate a single `ChunkInfo` in the filesystem at a path
    pub fn pre_allocate_chunk(
        &mut self,
        path: &Path,
        chunk_info: &ChunkInfo,
        offset: u64,
    ) -> Result<(), Error> {
        // TODO is there a way to improve this?
        // If already exists do nothing, kinda slow
        if let Some(ifcs) = self.data.get_vec(&chunk_info.hash) {
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

        let mut offset = 0;

        // Prepare all InFileChunk and add them to self.data
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

        // Then store items to persistence_path
        self.persist()?;
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

        // Then store items to persistence_path
        self.persist()?;

        for chunk in &item.chunks {
            if let Some(infile_chunks) = self.data.clone().get_vec(&chunk.hash) {
                // FIXME should not clone
                for infile_chunk in infile_chunks {
                    if infile_chunk.path == path {
                        self.data.remove(&chunk.hash);
                    }
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
        self.links.get(hash).cloned().or(self.get_data(hash))
    }

    fn size(&self) -> u64 {
        0 // TODO
    }

    /// Insert chunk into storage, requires an item to have been created with the appropriate chunks to be preallocate
    fn store_chunk(&mut self, hash: Hash, chunk: &[u8]) -> Option<Arc<Node>> {
        let infile_chunks = self.data.get_vec_mut(&hash)?;
        for infile_chunk in infile_chunks {
            tracing::trace!("infile chunk {infile_chunk:?}");
            infile_chunk
                .write(&hash, chunk, self.handles_map.get_mut(&infile_chunk.path)?)
                .inspect(|()| {
                    tracing::trace!(
                        "Written infile chunk {hash} to {}",
                        infile_chunk.path.to_string_lossy()
                    );
                })
                .inspect_err(|e| {
                    tracing::error!(
                        "Cannot write infile chunk to {}: {e} ",
                        infile_chunk.path.to_string_lossy()
                    );
                })
                .ok()?;
        }
        self.persist().ok()?;
        Some(Arc::new(Node::Stored {
            hash,
            data: Arc::new(chunk.to_vec()),
        }))
    }

    /// May only link chunks by adding items. Dummy implementation always returning None
    fn store_link(&mut self, hash: Hash, left: Arc<Node>, right: Arc<Node>) -> Option<Arc<Node>> {
        let size = left.size() + right.size();
        let res = self
            .links
            .try_insert(
                hash,
                Arc::new(Node::Parent {
                    hash,
                    left,
                    right,
                    size,
                }),
            )
            .map_or_else(|e| (*e.entry.get()).clone(), |x| (*x).clone());
        self.persist().ok()?;
        Some(res)
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
        create_dir_all(path.parent()?).ok()?;
        self.pre_allocate_bytes(&path, &file).ok()?;
        tracing::info!("Preallocated on disk {:?}", path);

        let hash_tree = self.insert(file)?;
        let item = Item::new(name, path, revision, description, &hash_tree);
        tracing::debug!("New item: {item}");

        self.items.insert(item.clone());
        self.persist().ok()?;

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

        self.persist().ok()?;

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

        self.persist()?;

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
        item::tests::{make_ones_item, new_dummy_item},
        utils::testing::temp_path,
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
            storage.data.iter_all(),
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
        // check default doesn't panics, just in case
        let storage = FsStorage::default();
        print_fsstorage(&storage);

        // create storage in a temporary directory
        let tempdir = temp_path();
        let mut storage = FsStorage::new(tempdir.clone());

        // make an item with a know content, a single chunk of all zeros
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
        let mut storage = FsStorage::new(tempdir.clone());

        // TODO replace this with data including both a deterministic non-chunk_size-aligned pattern and repeated chunks
        let item = new_dummy_item::<FsStorage, 1u8, 1_000_000>(&mut storage).unwrap();
        println!("Created item: {item:?}");
        print_fsstorage(&storage);

        let stored = storage.get(&item.metadata.root.hash).unwrap().clone_data();

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
            let mut storage = FsStorage::new(tempdir.clone());

            // save item and hash
            item = Some(new_dummy_item::<FsStorage, 1u8, 1_000_000>(&mut storage).unwrap());
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
            assert_eq!(storage.items.len(), 1);
            let retrieved = storage.items.iter().next().unwrap();
            let item = item.unwrap();
            assert_eq!(retrieved.metadata, item.metadata);
            assert_eq!(retrieved.chunks, item.chunks);
            assert_eq!(retrieved.hashes, item.hashes);
        }

        // Check data: same as fs_storage_roundtrip
        {
            println!("{:?}", storage.chunks());
            let stored = storage.get(&item_hash.unwrap()).unwrap().clone_data();

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
}
