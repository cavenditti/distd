//! Verified, random-access file views over ordered chunk manifests.
//!
//! Opening a reader validates revision metadata without reading payloads. Callers
//! must authenticate the revision separately (publisher policy, signature and
//! rollback checks). A Merkle root is not a whole-file BLAKE3 digest.
//!
//! ```
//! use std::io::{self, Read, Seek, SeekFrom};
//! use distd_core::{blob::{BlobChunkSource, BlobReader}, item::Item};
//!
//! fn read_header(source: &impl BlobChunkSource, revision: &Item) -> io::Result<[u8; 64]> {
//!     let mut reader = BlobReader::new(source, revision)?;
//!     reader.seek(SeekFrom::Start(4096))?;
//!     let mut header = [0; 64];
//!     reader.read_exact(&mut header)?;
//!     Ok(header)
//! }
//! ```

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::{
    chunk_storage::{
        fs_storage::FsStorage, node::ArcChunk, validate_revision_metadata, ReceivedChunk,
    },
    chunks::ChunkInfo,
    hash::{hash, Hash},
    item::Item,
};

/// A source of individual resident chunks, independent of complete hash trees.
///
/// Implementations should read only the requested chunk, check its declared size
/// before allocating, and return [`MissingChunk`] when it is unavailable. The
/// reader checks both length and BLAKE3 identity before exposing any bytes.
pub trait BlobChunkSource {
    fn read_chunk(&self, expected: &ChunkInfo) -> io::Result<ArcChunk>;
}

/// Identifies a chunk that a caller may fetch before retrying the same read.
/// Available through [`io::Error::get_ref`] on a `NotFound` error.
#[derive(Debug, thiserror::Error)]
#[error("chunk {chunk:?} is not resident")]
pub struct MissingChunk {
    pub chunk: ChunkInfo,
}

pub(crate) fn missing_chunk(chunk: &ChunkInfo) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, MissingChunk { chunk: *chunk })
}

pub(crate) fn invalid_chunk_size() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "chunk size differs from manifest",
    )
}

/// A seekable, read-only view of one file in a validated revision.
///
/// The reader borrows its source and manifest, indexes actual chunk sizes (also
/// for content-defined chunking), and retains at most one verified chunk. The
/// offset index uses O(number of file chunks) memory; payload retention is bounded
/// by the largest selected chunk. Storage backends may have their own caches.
/// Missing chunks do not prevent opening or reading other regions. No network
/// fetch, materialization, or activation takes place here.
pub struct BlobReader<'a, S: BlobChunkSource + ?Sized> {
    source: &'a S,
    chunks: &'a [ChunkInfo],
    ends: Vec<u64>,
    position: u64,
    cached: Option<(ChunkInfo, ArcChunk)>,
}

impl<'a, S: BlobChunkSource + ?Sized> BlobReader<'a, S> {
    /// Open a single-file artifact. Directory artifacts require [`Self::open_file`].
    pub fn new(source: &'a S, item: &'a Item) -> io::Result<Self> {
        validate_revision_metadata(item)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if !item.manifest.entries.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "select a file within the directory artifact",
            ));
        }
        Self::from_chunks(source, &item.chunks)
    }

    /// Open an exact, canonical relative path from a directory manifest.
    /// The path is a manifest lookup, never a host filesystem path.
    pub fn open_file(source: &'a S, item: &'a Item, relative_path: &str) -> io::Result<Self> {
        validate_revision_metadata(item)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if item.manifest.entries.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "artifact is not a directory",
            ));
        }
        let entry = item
            .manifest
            .entries
            .iter()
            .find(|entry| entry.relative_path == relative_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file is not in manifest"))?;
        // Revision validation checked these bounds and the file's declared size.
        Self::from_chunks(
            source,
            &item.chunks[entry.chunk_range.0 as usize..entry.chunk_range.1 as usize],
        )
    }

    fn from_chunks(source: &'a S, chunks: &'a [ChunkInfo]) -> io::Result<Self> {
        let mut ends = Vec::new();
        ends.try_reserve_exact(chunks.len())
            .map_err(io::Error::other)?;
        let mut end = 0u64;
        for chunk in chunks {
            usize::try_from(chunk.size).map_err(|_| invalid_chunk_size())?;
            if chunk.size == 0 && chunk.hash != hash(&[]) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty chunk has a nonempty identity",
                ));
            }
            end = end.checked_add(chunk.size).ok_or_else(invalid_chunk_size)?;
            ends.push(end);
        }
        Ok(Self {
            source,
            chunks,
            ends,
            position: 0,
            cached: None,
        })
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.ends.last().copied().unwrap_or(0)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the ordered slice of [`ChunkInfo`] descriptors for this file.
    #[must_use]
    pub fn chunks(&self) -> &'a [ChunkInfo] {
        self.chunks
    }

    /// Return the cumulative byte end offsets (prefix sum of chunk sizes) for this file.
    #[must_use]
    pub fn chunk_ends(&self) -> &[u64] {
        &self.ends
    }

    /// Resolve the exact subset of `(file_chunk_index, ChunkInfo)` intersecting `[offset, offset + len)`
    /// in $O(\log N + K)$ time via binary search over cumulative chunk offsets.
    #[must_use]
    pub fn chunks_for_range(&self, offset: u64, len: usize) -> Vec<(usize, ChunkInfo)> {
        if len == 0 || offset >= self.len() {
            return Vec::new();
        }
        let range_end = offset.saturating_add(len as u64).min(self.len());
        let first_idx = self.ends.partition_point(|end| *end <= offset);
        let mut selected = Vec::new();
        for idx in first_idx..self.chunks.len() {
            let chunk_start = if idx == 0 { 0 } else { self.ends[idx - 1] };
            if chunk_start >= range_end {
                break;
            }
            selected.push((idx, self.chunks[idx]));
        }
        selected
    }

    /// Return the subset of `(file_chunk_index, ChunkInfo)` intersecting `[offset, offset + len)`
    /// that are not currently resident in the underlying [`BlobChunkSource`].
    pub fn missing_chunks_for_range(
        &self,
        offset: u64,
        len: usize,
    ) -> io::Result<Vec<(usize, ChunkInfo)>> {
        let mut missing = Vec::new();
        for (idx, chunk) in self.chunks_for_range(offset, len) {
            match self.source.read_chunk(&chunk) {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    missing.push((idx, chunk));
                }
                Err(err) => return Err(err),
            }
        }
        Ok(missing)
    }

    /// Read from a file-relative offset without moving the stream cursor.
    ///
    /// Reads span chunk boundaries and stop at EOF. If a later chunk is missing
    /// or invalid, return the verified prefix already copied; retry at the next
    /// offset to observe that error. An error before any bytes are copied leaves
    /// the output untouched. Empty buffers and offsets at/past EOF return zero.
    pub fn read_at(&mut self, offset: u64, output: &mut [u8]) -> io::Result<usize> {
        let mut copied = 0;
        while copied < output.len() {
            // Each successful read stays within len(), so this cannot overflow.
            match self.read_one_at(offset + copied as u64, &mut output[copied..]) {
                Ok(0) => break,
                Ok(count) => copied += count,
                Err(_) if copied > 0 => break,
                Err(error) => return Err(error),
            }
        }
        Ok(copied)
    }

    fn read_one_at(&mut self, offset: u64, output: &mut [u8]) -> io::Result<usize> {
        if offset >= self.len() {
            return Ok(0);
        }
        let index = self.ends.partition_point(|end| *end <= offset);
        let expected = self.chunks[index];
        if self.cached.as_ref().map(|(chunk, _)| *chunk) != Some(expected) {
            // Drop the previous backing before asking the source for another.
            self.cached = None;
            let bytes = self.source.read_chunk(&expected)?;
            if bytes.len() as u64 != expected.size {
                return Err(invalid_chunk_size());
            }
            if hash(bytes.as_slice()) != expected.hash {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("chunk {} failed BLAKE3 verification", expected.hash),
                ));
            }
            self.cached = Some((expected, bytes));
        }
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        let within = (offset - start) as usize;
        let bytes = &self.cached.as_ref().expect("verified chunk is cached").1;
        let count = output.len().min(bytes.len() - within);
        output[..count].copy_from_slice(&bytes[within..within + count]);
        Ok(count)
    }
}

impl<S: BlobChunkSource + ?Sized> Read for BlobReader<'_, S> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = self.read_at(self.position, output)?;
        self.position += count as u64;
        Ok(count)
    }
}

impl<S: BlobChunkSource + ?Sized> Seek for BlobReader<'_, S> {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::End(delta) => self.len().checked_add_signed(delta),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek offset out of range"))?;
        self.position = position;
        Ok(position)
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
}

/// On-demand fetcher for missing chunks identified during a lazy range read.
pub trait OnDemandChunkFetcher {
    /// Fetch all requested `missing` chunks and return their verified payloads.
    fn fetch_chunks(&self, missing: &[ChunkInfo]) -> io::Result<Vec<(ChunkInfo, ReceivedChunk)>>;
}

impl<F> OnDemandChunkFetcher for F
where
    F: Fn(&[ChunkInfo]) -> io::Result<Vec<(ChunkInfo, ReceivedChunk)>>,
{
    fn fetch_chunks(&self, missing: &[ChunkInfo]) -> io::Result<Vec<(ChunkInfo, ReceivedChunk)>> {
        (self)(missing)
    }
}

/// Wraps a [`BlobChunkSource`] with an interior-mutable cache of on-demand fetched chunks.
pub struct CachedBlobChunkSource<'a, S: BlobChunkSource + ?Sized> {
    base: &'a S,
    fetched: RwLock<HashMap<Hash, ArcChunk>>,
}

impl<'a, S: BlobChunkSource + ?Sized> CachedBlobChunkSource<'a, S> {
    #[must_use]
    pub fn new(base: &'a S) -> Self {
        Self {
            base,
            fetched: RwLock::new(HashMap::new()),
        }
    }

    /// Verify and insert a fetched chunk into the overlay cache.
    pub fn insert_verified(&self, expected: &ChunkInfo, chunk: ReceivedChunk) -> io::Result<()> {
        if chunk.len() as u64 != expected.size {
            return Err(invalid_chunk_size());
        }
        if hash(chunk.as_ref()) != expected.hash {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("fetched chunk {} failed BLAKE3 verification", expected.hash),
            ));
        }
        self.fetched
            .write()
            .unwrap()
            .insert(expected.hash, Arc::new(chunk.into_vec()));
        Ok(())
    }

    /// Return true if `expected` is available in either the overlay cache or the base source.
    pub fn has_chunk(&self, expected: &ChunkInfo) -> io::Result<bool> {
        if self.fetched.read().unwrap().contains_key(&expected.hash) {
            return Ok(true);
        }
        match self.base.read_chunk(expected) {
            Ok(_) => Ok(true),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(err),
        }
    }
}

impl<S: BlobChunkSource + ?Sized> BlobChunkSource for CachedBlobChunkSource<'_, S> {
    fn read_chunk(&self, expected: &ChunkInfo) -> io::Result<ArcChunk> {
        if let Some(cached) = self.fetched.read().unwrap().get(&expected.hash).cloned() {
            if cached.len() as u64 != expected.size {
                return Err(invalid_chunk_size());
            }
            return Ok(cached);
        }
        self.base.read_chunk(expected)
    }
}

/// Hybrid lazy hydration bridge supporting:
/// - **Mode A (`FUSE_PASSTHROUGH` fast-path)**: when the whole-file CAS blob is already resident
///   in `FsStorage` (`.distd-activation-v1/blobs/<prefix>/<blob_hash>`), opens and serves reads
///   directly from the native backing file descriptor (`open_passthrough_file`).
/// - **On-Demand Range Fetching**: when the CAS blob is not yet resident, resolves the exact
///   intersecting chunks for `[offset, offset + len)` in $O(\log N)$ time, fetches only the
///   missing chunks via [`OnDemandChunkFetcher`], verifies their BLAKE3 digests, and serves the
///   range read while allowing full blob promotion into the CAS blob store (`hydrate_to_cas_blob`).
pub struct LazyBlobHydrator<'a, S: BlobChunkSource + ?Sized, F: OnDemandChunkFetcher> {
    source: CachedBlobChunkSource<'a, S>,
    fetcher: F,
    file_chunks: &'a [ChunkInfo],
    ends: Vec<u64>,
    blob_hash: Hash,
    cas_storage: Option<&'a FsStorage>,
    passthrough_file: Option<File>,
}

impl<'a, S: BlobChunkSource + ?Sized, F: OnDemandChunkFetcher> LazyBlobHydrator<'a, S, F> {
    /// Open a lazy hydrator for a single-file [`Item`].
    pub fn new(
        source: &'a S,
        fetcher: F,
        item: &'a Item,
        cas_storage: Option<&'a FsStorage>,
    ) -> io::Result<Self> {
        let probe = BlobReader::new(source, item)?;
        let ends = probe.chunk_ends().to_vec();
        let file_chunks = probe.chunks();
        Self::from_validated_parts(source, fetcher, file_chunks, ends, cas_storage)
    }

    /// Open a lazy hydrator for a relative file path inside a directory [`Item`].
    pub fn open_file(
        source: &'a S,
        fetcher: F,
        item: &'a Item,
        relative_path: &str,
        cas_storage: Option<&'a FsStorage>,
    ) -> io::Result<Self> {
        let probe = BlobReader::open_file(source, item, relative_path)?;
        let ends = probe.chunk_ends().to_vec();
        let file_chunks = probe.chunks();
        Self::from_validated_parts(source, fetcher, file_chunks, ends, cas_storage)
    }

    fn from_validated_parts(
        source: &'a S,
        fetcher: F,
        file_chunks: &'a [ChunkInfo],
        ends: Vec<u64>,
        cas_storage: Option<&'a FsStorage>,
    ) -> io::Result<Self> {
        let blob_hash = FsStorage::file_blob_hash(file_chunks)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let total_size = ends.last().copied().unwrap_or(0);
        let passthrough_file = match cas_storage {
            Some(storage) => storage
                .open_cas_blob(&blob_hash, total_size)
                .map_err(io::Error::other)?,
            None => None,
        };
        Ok(Self {
            source: CachedBlobChunkSource::new(source),
            fetcher,
            file_chunks,
            ends,
            blob_hash,
            cas_storage,
            passthrough_file,
        })
    }

    /// Content-addressed whole-file blob hash for this file.
    #[must_use]
    pub fn blob_hash(&self) -> Hash {
        self.blob_hash
    }

    /// Logical file size in bytes.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.ends.last().copied().unwrap_or(0)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return a cloned backing [`File`] handle suitable for `FUSE_DEV_IOC_BACKING_OPEN` /
    /// `FOPEN_PASSTHROUGH` if the whole-file CAS blob is already materialized on disk.
    pub fn open_passthrough_file(&self) -> io::Result<Option<File>> {
        if let Some(file) = &self.passthrough_file {
            return file.try_clone().map(Some);
        }
        if let Some(storage) = self.cas_storage {
            return storage
                .open_cas_blob(&self.blob_hash, self.len())
                .map_err(io::Error::other);
        }
        Ok(None)
    }

    /// Read a byte range `[offset, offset + output.len())`, serving directly from the native
    /// CAS blob file when resident, or fetching only the missing intersecting chunks on demand.
    pub fn read_at(&mut self, offset: u64, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || offset >= self.len() {
            return Ok(0);
        }
        if self.passthrough_file.is_none() {
            if let Some(storage) = self.cas_storage {
                self.passthrough_file = storage
                    .open_cas_blob(&self.blob_hash, self.len())
                    .map_err(io::Error::other)?;
            }
        }
        if let Some(file) = &self.passthrough_file {
            let max_len = (self.len() - offset).min(output.len() as u64) as usize;
            let target = &mut output[..max_len];
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.read_exact_at(target, offset)?;
            }
            #[cfg(not(unix))]
            {
                let mut cloned = file.try_clone()?;
                cloned.seek(SeekFrom::Start(offset))?;
                cloned.read_exact(target)?;
            }
            return Ok(max_len);
        }

        // Determine the exact intersecting chunks in O(log N) and batch-fetch any missing ones.
        let range_end = offset.saturating_add(output.len() as u64).min(self.len());
        let first_idx = self.ends.partition_point(|end| *end <= offset);
        let mut missing = Vec::new();
        let mut seen_hashes = HashSet::new();
        for idx in first_idx..self.file_chunks.len() {
            let chunk_start = if idx == 0 { 0 } else { self.ends[idx - 1] };
            if chunk_start >= range_end {
                break;
            }
            let chunk = self.file_chunks[idx];
            if !self.source.has_chunk(&chunk)? && seen_hashes.insert(chunk.hash) {
                missing.push(chunk);
            }
        }
        if !missing.is_empty() {
            let fetched = self.fetcher.fetch_chunks(&missing)?;
            let mut received_by_hash: HashMap<Hash, ReceivedChunk> = fetched
                .into_iter()
                .map(|(info, data)| (info.hash, data))
                .collect();
            for expected in &missing {
                let data = received_by_hash
                    .remove(&expected.hash)
                    .ok_or_else(|| missing_chunk(expected))?;
                self.source.insert_verified(expected, data)?;
            }
        }

        let mut reader = BlobReader::from_chunks(&self.source, self.file_chunks)?;
        reader.read_at(offset, output)
    }

    /// Fetch any remaining missing chunks of this file and publish the complete verified blob into
    /// `storage`'s native CAS blob store (`.distd-activation-v1/blobs/<prefix>/<blob_hash>`),
    /// upgrading this hydrator to native passthrough mode for all subsequent reads.
    pub fn hydrate_to_cas_blob(&mut self, storage: &FsStorage) -> io::Result<PathBuf> {
        if storage.has_cas_blob(&self.blob_hash, self.len()) {
            let path = storage.cas_blob_path(&self.blob_hash);
            if self.passthrough_file.is_none() {
                self.passthrough_file = Some(File::open(&path)?);
            }
            return Ok(path);
        }

        let mut missing = Vec::new();
        let mut seen_hashes = HashSet::new();
        for chunk in self.file_chunks {
            if !self.source.has_chunk(chunk)? && seen_hashes.insert(chunk.hash) {
                missing.push(*chunk);
            }
        }
        if !missing.is_empty() {
            let fetched = self.fetcher.fetch_chunks(&missing)?;
            let mut received_by_hash: HashMap<Hash, ReceivedChunk> = fetched
                .into_iter()
                .map(|(info, data)| (info.hash, data))
                .collect();
            for expected in &missing {
                let data = received_by_hash
                    .remove(&expected.hash)
                    .ok_or_else(|| missing_chunk(expected))?;
                self.source.insert_verified(expected, data)?;
            }
        }

        let path = storage.materialize_cas_blob_from_source(self.file_chunks, &self.source)?;
        self.passthrough_file = Some(File::open(&path)?);
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_storage::hashmap_storage::HashMapStorage;
    use crate::chunk_storage::ChunkStorage;
    use crate::chunks::ChunkAlgorithm;
    use crate::utils::testing::temp_path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct PartialMemorySource {
        resident: RwLock<HashMap<Hash, ArcChunk>>,
    }

    impl PartialMemorySource {
        fn new() -> Self {
            Self {
                resident: RwLock::new(HashMap::new()),
            }
        }

        fn insert(&self, data: &[u8]) {
            self.resident
                .write()
                .unwrap()
                .insert(hash(data), Arc::new(data.to_vec()));
        }
    }

    impl BlobChunkSource for PartialMemorySource {
        fn read_chunk(&self, expected: &ChunkInfo) -> io::Result<ArcChunk> {
            self.resident
                .read()
                .unwrap()
                .get(&expected.hash)
                .cloned()
                .ok_or_else(|| missing_chunk(expected))
        }
    }

    #[test]
    fn lazy_blob_hydrator_fetches_only_intersecting_chunks_and_promotes_to_cas_passthrough() {
        let full_data = b"00001111222233334444555566667777";
        let builder = HashMapStorage::default();
        let algo = ChunkAlgorithm::fixed_aligned(4, 1);
        let (root_node, chunks) = builder.chunk_tree(full_data, algo).unwrap();
        assert_eq!(chunks.len(), 8);
        let item = Item::make(
            "lazy-pkg".to_string(),
            PathBuf::from("lazy.bin"),
            1,
            None,
            root_node.chunk_info(),
            chunks.clone(),
            algo,
        )
        .unwrap();

        // Only chunk 2 ("2222") is initially resident locally.
        let local_source = PartialMemorySource::new();
        local_source.insert(b"2222");

        let remote_chunks: HashMap<Hash, Vec<u8>> = chunks
            .iter()
            .enumerate()
            .map(|(idx, info)| (info.hash, full_data[idx * 4..(idx + 1) * 4].to_vec()))
            .collect();

        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let fetched_chunk_count = Arc::new(AtomicUsize::new(0));
        let fetch_calls_clone = fetch_calls.clone();
        let fetched_chunk_count_clone = fetched_chunk_count.clone();

        let root_dir = temp_path();
        let cas_storage = FsStorage::new(root_dir.clone()).unwrap();

        let mut hydrator = LazyBlobHydrator::new(
            &local_source,
            move |missing: &[ChunkInfo]| {
                fetch_calls_clone.fetch_add(1, Ordering::SeqCst);
                fetched_chunk_count_clone.fetch_add(missing.len(), Ordering::SeqCst);
                missing
                    .iter()
                    .map(|info| {
                        let bytes = remote_chunks
                            .get(&info.hash)
                            .ok_or_else(|| missing_chunk(info))?;
                        Ok((*info, ReceivedChunk::copy_from_slice(bytes)))
                    })
                    .collect()
            },
            &item,
            Some(&cas_storage),
        )
        .unwrap();

        // Initially the CAS blob is not materialized, so passthrough FD is None.
        assert!(hydrator.open_passthrough_file().unwrap().is_none());

        // Read range [6, 14) -> intersects chunks 1 ("1111"), 2 ("2222"), 3 ("3333").
        // Since chunk 2 is already resident, only chunks 1 and 3 should be fetched!
        let mut buf = [0u8; 8];
        let n = hydrator.read_at(6, &mut buf).unwrap();
        assert_eq!(n, 8);
        assert_eq!(&buf, b"11222233");
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fetched_chunk_count.load(Ordering::SeqCst), 2);

        // Re-reading the same range does not trigger any network fetch.
        let mut buf2 = [0u8; 8];
        assert_eq!(hydrator.read_at(6, &mut buf2).unwrap(), 8);
        assert_eq!(&buf2, b"11222233");
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);

        // Now promote/hydrate the full blob into FsStorage's CAS blob store.
        // Only the remaining 5 missing chunks (0, 4, 5, 6, 7) should be fetched!
        let cas_path = hydrator.hydrate_to_cas_blob(&cas_storage).unwrap();
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 2);
        assert_eq!(fetched_chunk_count.load(Ordering::SeqCst), 2 + 5);
        assert_eq!(std::fs::read(&cas_path).unwrap(), full_data);

        // Subsequent opens/reads use the native passthrough FD directly without calling fetcher.
        assert!(hydrator.open_passthrough_file().unwrap().is_some());
        let mut full_buf = vec![0u8; full_data.len()];
        assert_eq!(hydrator.read_at(0, &mut full_buf).unwrap(), full_data.len());
        assert_eq!(full_buf, full_data);
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 2);
    }
}
