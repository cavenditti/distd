//! common chunks and hash-tree data structs


use serde::{Deserialize, Serialize};

use crate::hash::Hash;

/// Chunk size in bytes
/// It may useful to increase this in order to print hash tree when debugging
//pub const CHUNK_SIZE: usize = blake3::guts::CHUNK_LEN;
pub const CHUNK_SIZE: usize = 256 * 1024;

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
