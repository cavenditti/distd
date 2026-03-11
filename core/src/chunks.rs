//! common chunks and hash-tree data structs


use serde::{Deserialize, Serialize};

use crate::hash::Hash;

/// Chunk size in bytes
/// It may useful to increase this in order to print hash tree when debugging
//pub const CHUNK_SIZE: usize = blake3::guts::CHUNK_LEN;
pub const CHUNK_SIZE: usize = 256 * 1024;
pub const CHUNK_SIZE_U64: u64 = CHUNK_SIZE as u64;

/// Owned chunk
pub type OwnedChunk = Vec<u8>;

//pub type Size = u64;

/// Node in an hash-tree
/// It may be a parent node with two children or a leaf node with owned data
pub trait HashTreeNode {
    /// Get hash of node
    fn hash(&self) -> &Hash;

    /// Compute sum size in bytes of all descending chunks
    fn size(&self) -> u64;

    /// Get contained data, returns None if is not Stored
    fn stored_data(&self) -> Option<&OwnedChunk>;

    /// Get mutable contained data, returns None if is not Stored
    fn stored_data_mut(&mut self) -> Option<&mut OwnedChunk>;

    /// Get owned contained data, returns None if is not `Stored`
    fn stored_data_owned(self) -> Option<OwnedChunk>;

    /// Get contained data, returns None if is not Parent
    fn children(&self) -> Option<(&Self, &Self)>;

    /// Get diff sub-tree: required tree to reconstruct current node if one has the `hashes`
    #[must_use]
    fn find_diff(&self, hashes: &[Hash]) -> Self;

    /// Flatten the tree into an iterator on chunks
    fn flatten_iter(self) -> Box<dyn Iterator<Item = Vec<u8>>>;

    /// Wheter the subtree has any missing node
    ///
    /// A default implementation si provided to skip this if the type cannot have missing nodes
    fn is_complete(&self) -> bool {
        true
    }
}

/// Seralizable view of an hash tree node, only contains size and hash
#[derive(Debug, Clone, Copy, Serialize, Deserialize, std::hash::Hash, PartialEq, Eq)]
pub struct ChunkInfo {
    // progressive unique id provided by the storage ??
    //pub id: u64,
    // Chunk size
    pub size: u64,
    // Chunk hash
    pub hash: Hash,
}

impl ChunkInfo {
    #[allow(dead_code)] // TODO check if it's needed
    fn is_leaf(&self) -> bool {
        //self.children.is_none()
        self.size == CHUNK_SIZE as u64
    }
}

// ─── Adaptive Chunking (Step 8) ─────────────────────────────────────────────

/// Chunking algorithm identifier, stored in manifests.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, std::hash::Hash)]
pub enum ChunkAlgorithm {
    /// Fixed-size 256 KiB chunks (current default).
    Fixed256K,
    /// Content-defined chunking via FastCDC-like rolling hash.
    /// Parameters: (min_size, avg_size, max_size) in bytes.
    ContentDefined {
        min_size: u32,
        avg_size: u32,
        max_size: u32,
    },
}

impl Default for ChunkAlgorithm {
    fn default() -> Self {
        Self::Fixed256K
    }
}

/// Trait for splitting data into chunks.
pub trait Chunker {
    /// Split `data` into a vector of byte slices (boundaries).
    /// Returns a vector of (offset, length) pairs.
    fn chunk_boundaries(&self, data: &[u8]) -> Vec<(usize, usize)>;
}

/// Fixed-size chunker (current 256 KiB).
pub struct FixedChunker;

impl Chunker for FixedChunker {
    fn chunk_boundaries(&self, data: &[u8]) -> Vec<(usize, usize)> {
        let mut boundaries = Vec::with_capacity(data.len() / CHUNK_SIZE + 1);
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + CHUNK_SIZE).min(data.len());
            boundaries.push((offset, end - offset));
            offset = end;
        }
        if boundaries.is_empty() {
            boundaries.push((0, 0));
        }
        boundaries
    }
}

/// Content-defined chunker using a simple Gear-hash rolling window.
/// This provides better dedup on shifted/edited binaries.
pub struct ContentDefinedChunker {
    pub min_size: usize,
    pub avg_size: usize,
    pub max_size: usize,
}

impl Default for ContentDefinedChunker {
    fn default() -> Self {
        Self {
            min_size: 64 * 1024,    // 64 KiB min
            avg_size: 256 * 1024,   // 256 KiB avg
            max_size: 1024 * 1024,  // 1 MiB max
        }
    }
}

impl Chunker for ContentDefinedChunker {
    fn chunk_boundaries(&self, data: &[u8]) -> Vec<(usize, usize)> {
        if data.is_empty() {
            return vec![(0, 0)];
        }

        let mut boundaries = Vec::new();
        let mut offset = 0;
        // Gear hash table — a fixed random mapping for each byte value
        let gear: [u64; 256] = gear_table();
        let mask = self.avg_size.next_power_of_two() as u64 - 1;

        while offset < data.len() {
            let chunk_start = offset;
            let min_end = (offset + self.min_size).min(data.len());
            let max_end = (offset + self.max_size).min(data.len());

            // Skip to min_size
            offset = min_end;
            let mut hash: u64 = 0;

            while offset < max_end {
                hash = (hash << 1).wrapping_add(gear[data[offset] as usize]);
                offset += 1;
                if hash & mask == 0 {
                    break;
                }
            }

            // If we reached max_end without a boundary, cut here anyway
            if offset >= max_end {
                offset = max_end;
            }

            boundaries.push((chunk_start, offset - chunk_start));
        }
        boundaries
    }
}

/// Deterministic gear hash table (seeded from a fixed value).
fn gear_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    // Simple LCG-based generation for deterministic, well-distributed values
    let mut state: u64 = 0x1234_5678_9ABC_DEF0;
    for entry in table.iter_mut() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *entry = state;
    }
    table
}
