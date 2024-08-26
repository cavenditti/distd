//! common chunks and hash-tree data structs

use blake3::Hash;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::utils::serde::hashes::{deserialize_hash, serialize_hash};

// It may useful to increase this in order to print hash tree when debugging
//pub const CHUNK_SIZE: usize = 4 * 1024;
pub const CHUNK_SIZE: usize = blake3::guts::CHUNK_LEN;

pub type RawChunk = Arc<Vec<u8>>;
pub type OwnedChunk = Vec<u8>;
//pub type Size = u64;

pub trait HashTreeNode {
    /// Get hash of node
    fn get_hash(&self) -> &Hash;

    /// Compute sum size in bytes of all descending chunks
    fn get_size(&self) -> u32;

    /// Get contained data, returns None if is not Stored
    fn get_stored_data(&self) -> Option<&OwnedChunk>;

    /// Get contained data, returns None if is not Parent
    fn get_children(&self) -> Option<(&Self, &Self)>;
}

/// Seralizable view of the hash tree, fully owns stored chunks and subtrees
#[derive(Debug, Clone, Serialize, Deserialize, std::hash::Hash, PartialEq, Eq)]
pub enum OwnedHashTreeNode {
    Parent {
        size: u32,
        #[serde(
            serialize_with = "serialize_hash",
            deserialize_with = "deserialize_hash"
        )]
        hash: Hash,
        left: Box<OwnedHashTreeNode>,
        right: Box<OwnedHashTreeNode>,
    },
    Stored {
        #[serde(
            serialize_with = "serialize_hash",
            deserialize_with = "deserialize_hash"
        )]
        hash: Hash,
        data: OwnedChunk,
    },
}

impl HashTreeNode for OwnedHashTreeNode {
    fn get_hash(&self) -> &Hash {
        match self {
            Self::Parent { hash, .. } => hash,
            Self::Stored { hash, .. } => hash,
        }
    }

    fn get_size(&self) -> u32 {
        match self {
            Self::Parent { size, .. } => *size,
            Self::Stored { data, .. } => data.len() as u32,
        }
    }

    fn get_children(&self) -> Option<(&Self, &Self)> {
        match self {
            Self::Parent { left, right, .. } => Some((left, right)),
            Self::Stored { .. } => None,
        }
    }

    fn get_stored_data(&self) -> Option<&OwnedChunk> {
        match self {
            Self::Parent { .. } => None,
            Self::Stored { data, .. } => Some(data),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, std::hash::Hash, PartialEq, Eq)]
pub struct ChunkInfo {
    // progressive unique id provided by the storage ??
    //pub id: u64,
    // Chunk size
    pub size: u32,
    // Chunk hash
    #[serde(
        serialize_with = "serialize_hash",
        deserialize_with = "deserialize_hash"
    )]
    pub hash: Hash,
}

impl ChunkInfo {
    #[allow(dead_code)] // TODO check if it's needed
    fn is_leaf(&self) -> bool {
        //self.children.is_none()
        self.size == CHUNK_SIZE as u32
    }
}
