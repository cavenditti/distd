//! PPSPP-style possession bitfield for chunk-level sync.

use serde::{Deserialize, Serialize};

use crate::chunk_storage::ChunkStorage;
use crate::hash::Hash;
use crate::item::Manifest;

/// A bitfield tracking which chunks a peer possesses.
/// Bit N = 1 means chunk index N is locally available.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Bitfield {
    /// Packed bits, little-endian byte order. Bit 0 of byte 0 = chunk 0.
    data: Vec<u8>,
    /// Total number of chunks this bitfield covers.
    count: u32,
}

impl Bitfield {
    /// Create an all-zeros (empty) bitfield for `count` chunks.
    #[must_use]
    pub fn empty(count: u32) -> Self {
        let bytes = ((count as usize) + 7) / 8;
        Self {
            data: vec![0u8; bytes],
            count,
        }
    }

    /// Create an all-ones (complete) bitfield for `count` chunks.
    #[must_use]
    pub fn full(count: u32) -> Self {
        let bytes = ((count as usize) + 7) / 8;
        let mut data = vec![0xFFu8; bytes];
        // Clear trailing bits beyond count
        let trailing = (count as usize) % 8;
        if trailing != 0 && !data.is_empty() {
            let last = data.len() - 1;
            data[last] = (1u8 << trailing) - 1;
        }
        Self { data, count }
    }

    /// Build a bitfield by scanning local storage for which chunks exist.
    ///
    /// Uses the storage's `chunk_list` to get the ordered chunk hashes for the manifest,
    /// then checks which ones are locally available.
    pub fn from_storage<S: ChunkStorage>(storage: &S, manifest: &Manifest, chunk_hashes: &[Hash]) -> Self {
        let mut bf = Self::empty(manifest.chunk_count);
        for (i, hash) in chunk_hashes.iter().enumerate() {
            if storage.get(hash).is_some() {
                bf.set(i as u32);
            }
        }
        bf
    }

    /// Set bit at index.
    pub fn set(&mut self, index: u32) {
        debug_assert!(index < self.count);
        let byte = (index / 8) as usize;
        let bit = index % 8;
        self.data[byte] |= 1 << bit;
    }

    /// Test bit at index.
    #[must_use]
    pub fn has(&self, index: u32) -> bool {
        if index >= self.count {
            return false;
        }
        let byte = (index / 8) as usize;
        let bit = index % 8;
        (self.data[byte] >> bit) & 1 == 1
    }

    /// Return indices of chunks the peer does NOT have.
    #[must_use]
    pub fn missing_indices(&self) -> Vec<u32> {
        (0..self.count).filter(|&i| !self.has(i)).collect()
    }

    /// True if all bits are zero (peer has nothing).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.iter().all(|&b| b == 0)
    }

    /// True if all chunks are possessed.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_indices().is_empty()
    }

    /// Number of chunks.
    #[must_use]
    pub fn chunk_count(&self) -> u32 {
        self.count
    }

    /// Serialize to raw bytes for wire transfer.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.data.len());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.data);
        out
    }

    /// Deserialize from raw bytes.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        let count = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let expected_len = ((count as usize) + 7) / 8;
        if bytes.len() < 4 + expected_len {
            return None;
        }
        Some(Self {
            count,
            data: bytes[4..4 + expected_len].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bitfield() {
        let bf = Bitfield::empty(10);
        assert_eq!(bf.chunk_count(), 10);
        assert!(bf.is_empty());
        assert!(!bf.is_complete());
        assert_eq!(bf.missing_indices().len(), 10);
    }

    #[test]
    fn full_bitfield() {
        let bf = Bitfield::full(10);
        assert!(bf.is_complete());
        assert!(bf.missing_indices().is_empty());
        for i in 0..10 {
            assert!(bf.has(i));
        }
        // Bits beyond count should not be set
        assert!(!bf.has(10));
    }

    #[test]
    fn set_and_has() {
        let mut bf = Bitfield::empty(16);
        bf.set(0);
        bf.set(7);
        bf.set(15);
        assert!(bf.has(0));
        assert!(bf.has(7));
        assert!(bf.has(15));
        assert!(!bf.has(1));
        assert_eq!(bf.missing_indices().len(), 13);
    }

    #[test]
    fn roundtrip() {
        let mut bf = Bitfield::empty(33);
        bf.set(0);
        bf.set(32);
        let bytes = bf.to_bytes();
        let bf2 = Bitfield::from_bytes(&bytes).unwrap();
        assert_eq!(bf, bf2);
    }
}
