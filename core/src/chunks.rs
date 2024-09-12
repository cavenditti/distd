//! common chunks and hash-tree data structs

use std::{
    fmt::{Display, Write},
    str::FromStr,
    sync::Arc,
};

use blake3::Hash;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    hash::merge_hashes,
    utils::serde::hashes::{deserialize_hash, serialize_hash},
};

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

    /// Get mutable contained data, returns None if is not Stored
    fn stored_data_mut(&mut self) -> Option<&mut OwnedChunk>;

    /// Get owned contained data, returns None if is not `Stored`
    fn stored_data_owned(self) -> Option<OwnedChunk>;

    /// Get contained data, returns None if is not Parent
    fn children(&self) -> Option<(&Self, &Self)>;

    /// Get diff sub-tree: required tree to reconstruct current node if one has the `hashes`
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

    /// Node skipped in serialization
    Skipped {
        #[serde(
            serialize_with = "serialize_hash",
            deserialize_with = "deserialize_hash"
        )]
        hash: Hash,
        size: u32,
    },
}

impl HashTreeNode for OwnedHashTreeNode {
    /// Get hash of node
    fn hash(&self) -> &Hash {
        match self {
            Self::Parent { hash, .. } | Self::Stored { hash, .. } | Self::Skipped { hash, .. } => {
                hash
            }
        }
    }

    /// Compute sum size in bytes of all descending chunks
    fn size(&self) -> u32 {
        match self {
            Self::Parent { size, .. } | Self::Skipped { size, .. } => *size,
            Self::Stored { data, .. } => data.len() as u32,
        }
    }

    /// Get children, return None if is `Stored` or `Skipped`
    fn children(&self) -> Option<(&Self, &Self)> {
        match self {
            Self::Parent { left, right, .. } => Some((left, right)),
            Self::Stored { .. } | Self::Skipped { .. } => None,
        }
    }

    /// Get contained data, returns None if is not `Stored`
    fn stored_data(&self) -> Option<&OwnedChunk> {
        match self {
            Self::Parent { .. } | Self::Skipped { .. } => None,
            Self::Stored { data, .. } => Some(data),
        }
    }

    /// Get mutable contained data, returns None if is not `Stored`
    fn stored_data_mut(&mut self) -> Option<&mut OwnedChunk> {
        match self {
            Self::Parent { .. } | Self::Skipped { .. } => None,
            Self::Stored { data, .. } => Some(data),
        }
    }

    /// Get owned contained data, returns None if is not `Stored`
    fn stored_data_owned(self) -> Option<OwnedChunk> {
        match self {
            Self::Parent { .. } | Self::Skipped { .. } => None,
            Self::Stored { data, .. } => Some(data),
        }
    }

    /// Get diff sub-tree: required tree to reconstruct current node if one has the `hashes`
    fn find_diff(&self, hashes: &[Hash]) -> Self {
        if hashes.contains(self.hash()) {
            return Self::Skipped {
                hash: *self.hash(),
                size: self.size(),
            };
        }
        match self {
            Self::Parent {
                size,
                hash,
                left,
                right,
            } => {
                // Go down and recursively find diffs
                let left = left.find_diff(hashes);
                let right = right.find_diff(hashes);

                // When coming back up skip whole sub-trees if both children are skipped
                match (&left, &right) {
                    (Self::Skipped { .. }, Self::Skipped { .. }) => Self::Skipped {
                        hash: *hash,
                        size: *size,
                    },
                    _ => Self::Parent {
                        size: *size,
                        hash: *hash,
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                }
            }
            node => node.clone(),
        }
    }

    /// Flatten the tree into an iterator on chunks
    ///
    /// This is a recursive function that returns an iterator on the chunks of the tree
    ///
    /// # Returns
    /// An iterator on the chunks of the tree
    ///
    /// # Panics
    /// If the tree contains a `Skipped` node
    fn flatten_iter(self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        match self {
            Self::Stored { data, .. } => Box::new([data].into_iter()),
            Self::Parent { left, right, .. } => {
                Box::new(left.flatten_iter().chain(right.flatten_iter()))
            }
            Self::Skipped { .. } => panic!("Trying to flatten a `Skipped` node"),
        }
    }

    fn is_complete(&self) -> bool {
        match self {
            Self::Stored { .. } => true,
            Self::Skipped { .. } => false,
            Self::Parent { left, right, .. } => left.is_complete() && right.is_complete(),
        }
    }
}

impl Display for OwnedHashTreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let h_str = self.hash().to_string()[..8].to_string() + "..";
        match self {
            Self::Parent {
                size, left, right, ..
            } => {
                write!(
                    f,
                    "HashTreeNode::Parent {{ {h_str}, <LEFT: {}, RIGHT: {}>, {size}B }}",
                    left.to_string(),
                    right.to_string(),
                )
            }
            Self::Stored { data, .. } => {
                write!(
                    f,
                    "HashTreeNode::Stored {{ {h_str}, {}B }}",
                    data.len(),
                )
            }
            Self::Skipped { size, .. } => {
                write!(f, "HashTreeNode::Skipped {{ {h_str}, {size}B  }}")
            }
        }
    }
}

#[derive(Error, Debug)]
#[error("Expected {expected} instead")]
pub struct HashTreeNodeTypeError {
    pub expected: String,
}

#[derive(Error, Debug)]
#[error("Found skipped/missing node when one was expected")]
pub struct HashTreeNodeMissingError;

/// Flatten hash-tree chunks (`Stored` nodes) into a single vector of bytes
pub fn flatten(chunks: Vec<OwnedHashTreeNode>) -> Result<Vec<u8>, HashTreeNodeTypeError> {
    Ok(chunks
        .into_iter()
        // no flat_map here, as Result implements IntoIterator and errors just get ignored
        .map(|x| {
            x.stored_data_owned().ok_or(HashTreeNodeTypeError {
                expected: "Stored".into(),
            })
        })
        .collect::<Result<Vec<Vec<u8>>, HashTreeNodeTypeError>>()?
        .into_iter()
        .flatten()
        .collect())
}

/// Flatten hash-tree chunks (`Stored` nodes) into an iteratorn on bytes
pub fn flatten_iter(
    chunks: Vec<OwnedHashTreeNode>,
) -> Result<
    std::iter::Flatten<
        std::iter::Map<
            std::vec::IntoIter<OwnedHashTreeNode>,
            impl FnMut(OwnedHashTreeNode) -> Result<Vec<u8>, HashTreeNodeTypeError>,
        >,
    >,
    HashTreeNodeTypeError,
> {
    Ok(chunks
        .into_iter()
        // no flat_map here, as Result implements IntoIterator and errors just get ignored
        .map(|x| {
            x.stored_data_owned().ok_or(HashTreeNodeTypeError {
                expected: "Stored".into(),
            })
        })
        .into_iter()
        .flatten())
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

#[cfg(test)]
mod tests {
    use crate::hash::hash;

    use super::{flatten, OwnedHashTreeNode};

    #[test]
    fn test_flatten() {
        const L: usize = 2000;
        let b1 = vec![0; L];
        let b2 = vec![1; L];
        let l1 = b1.len();
        let l2 = b2.len();
        let n1 = OwnedHashTreeNode::Stored {
            hash: hash(b""),
            data: b1,
        };
        let n2 = OwnedHashTreeNode::Stored {
            hash: hash(b""),
            data: b2,
        };
        let flat = flatten(vec![n1, n2]).unwrap();

        assert_eq!(flat.len(), l1 + l2);

        for v in &flat[..L] {
            assert_eq!(*v, 0);
        }

        for v in &flat[L..] {
            assert_eq!(*v, 1);
        }
    }
}
