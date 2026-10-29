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

use std::io::{self, Read, Seek, SeekFrom};

use crate::{
    chunk_storage::{node::ArcChunk, validate_revision_metadata},
    chunks::ChunkInfo,
    hash::hash,
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
