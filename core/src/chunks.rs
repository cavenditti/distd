//! common chunks and hash-tree data structs

use blake3::Hash;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
//use ring::signature::Signature;

use serde::{Deserialize, Serialize};

use crate::utils::serde::hashes::{deserialize_hash, serialize_hash};

// It may useful to increase this in order to print hash tree when debugging
//pub const CHUNK_SIZE: usize = 4 * 1024;
pub const CHUNK_SIZE: usize = blake3::guts::CHUNK_LEN;

//pub type RawChunk = [u8; CHUNK_SIZE];
pub type RawChunk = Arc<Vec<u8>>;
pub type OwnedChunk = Vec<u8>;
pub type RawHash = [u8; 32];

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

trait ClonableHashTreeNode: HashTreeNode + Into<OwnedHashTreeNode> {} // Useful?

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
    /*
    #[serde(
        serialize_with = "serialize_opt_2tuple_hash",
        deserialize_with = "deserialize_opt_2tuple_hash"
    )]
    pub children: Option<(Hash, Hash)>,
    */
}

impl ChunkInfo {
    fn is_leaf(&self) -> bool {
        //self.children.is_none()
        self.size == CHUNK_SIZE as u32
    }
}

pub struct ChunksPack {
    chunk_size: usize,
    last_chunk_size: usize,
    hashes: Vec<Hash>, // We only keep hashes for chunks, they will then be retrieved from storage
}

//pub type ChunksMap = BTreeMap<u64, ChunkInfo>;
pub type ChunksMap = HashMap<PathBuf, ChunksPack>;
