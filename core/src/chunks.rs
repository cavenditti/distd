//! common chunks and hash-tree data structs

use blake3::Hash;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::utils::serde::hashes::{deserialize_hash, serialize_hash};

/// Chunk size in bytes
/// It may useful to increase this in order to print hash tree when debugging
pub const CHUNK_SIZE: usize = blake3::guts::CHUNK_LEN;
//pub const CHUNK_SIZE: usize = 4 * 1024;

/// Raw chunk reference
pub type RawChunk = Arc<Vec<u8>>;

/// Owned chunk
pub type OwnedChunk = Vec<u8>;

//pub type Size = u64;

/// Node in an hash-tree
/// It may be a parent node with two children or a leaf node with owned data
pub trait HashTreeNode {
    /// Get hash of node
    fn hash(&self) -> &Hash;

    /// Compute sum size in bytes of all descending chunks
    fn size(&self) -> u32;

    /// Get contained data, returns None if is not Stored
    fn stored_data(&self) -> Option<&OwnedChunk>;

    /// Get contained data, returns None if is not Parent
    fn children(&self) -> Option<(&Self, &Self)>;
}

/// Seralizable view of the hash tree, fully owns stored chunks and subtrees
#[derive(Debug, Clone, Serialize, Deserialize, std::hash::Hash, PartialEq, Eq)]
pub enum OwnedHashTreeNode {
    /// Parent node with two children
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

    /// Leaf node with owned stored data
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
    /// Get hash of node
    fn hash(&self) -> &Hash {
        match self {
            Self::Parent { hash, .. } | Self::Stored { hash, .. } => hash,
        }
    }

    /// Compute sum size in bytes of all descending chunks
    fn size(&self) -> u32 {
        match self {
            Self::Parent { size, .. } => *size,
            Self::Stored { data, .. } => data.len() as u32,
        }
    }

    /// Get children, return None if is `Stored`
    fn children(&self) -> Option<(&Self, &Self)> {
        match self {
            Self::Parent { left, right, .. } => Some((left, right)),
            Self::Stored { .. } => None,
        }
    }

    /// Get contained data, returns None if is not `Stored`
    fn stored_data(&self) -> Option<&OwnedChunk> {
        match self {
            Self::Parent { .. } => None,
            Self::Stored { data, .. } => Some(data),
        }
    }
}

/// Seralizable view of an hash tree node, only contains size and hash
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
