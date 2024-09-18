use std::{collections::HashSet, sync::Arc};

use serde::ser::{Serialize, SerializeStructVariant};

use crate::hash::Hash;
use crate::chunks::{ChunkInfo, HashTreeNode, OwnedHashTreeNode};

/// Arc reference to a raw byte chunk
pub type ArcChunk = Arc<Vec<u8>>;

/// This is the internal representation of the hash-tree
/// As it contains in-memory references, it is not meant to be serialized
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredChunkRef {
    Parent {
        hash: Hash,
        left: Arc<StoredChunkRef>,
        right: Arc<StoredChunkRef>,
    },
    Stored {
        hash: Hash,
        data: ArcChunk,
    },
}

impl Serialize for StoredChunkRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use base64::prelude::*;
        if serializer.is_human_readable() {
            match self {
                Self::Parent { left, right, .. } => {
                    let mut state =
                        serializer.serialize_struct_variant("StoredChunkRef", 0, "Parent", 2)?;
                    state.serialize_field("left", &left.hash().to_string())?;
                    state.serialize_field("right", &right.hash().to_string())?;
                    state.end()
                }
                Self::Stored { hash, data } => {
                    let mut state =
                        serializer.serialize_struct_variant("StoredChunkRef", 0, "Stored", 2)?;
                    state.serialize_field("hash", &hash.to_string())?;
                    state.serialize_field("data", &BASE64_STANDARD.encode(data.as_ref()))?;
                    state.end()
                }
            }
        } else {
            match self {
                Self::Parent { hash, left, right } => {
                    let mut state =
                        serializer.serialize_struct_variant("StoredChunkRef", 0, "Parent", 3)?;
                    state.serialize_field("hash", &hash.as_bytes())?;
                    state.serialize_field("left", &left)?;
                    state.serialize_field("right", &right)?;
                    state.end()
                }
                Self::Stored { hash, data } => {
                    let mut state =
                        serializer.serialize_struct_variant("StoredChunkRef", 0, "Stored", 2)?;
                    state.serialize_field("hash", &hash.as_bytes())?;
                    state.serialize_field("data", &*data.clone())?;
                    state.end()
                }
            }
        }
        // 3 is the number of fields in the struct.
    }
}

impl From<StoredChunkRef> for OwnedHashTreeNode {
    fn from(value: StoredChunkRef) -> Self {
        let size = value.size() as u32;
        match value {
            StoredChunkRef::Parent { hash, left, right } => OwnedHashTreeNode::Parent {
                size,
                hash,
                left: Box::new(OwnedHashTreeNode::from((*left).clone())),
                right: Box::new(OwnedHashTreeNode::from((*right).clone())),
            },
            StoredChunkRef::Stored { hash, data } => OwnedHashTreeNode::Stored {
                hash,
                data: (*data).clone(),
            },
        }
    }
}

impl TryFrom<OwnedHashTreeNode> for StoredChunkRef {
    type Error = OwnedHashTreeNode;

    fn try_from(value: OwnedHashTreeNode) -> Result<Self, Self::Error> {
        match value {
            OwnedHashTreeNode::Stored { hash, data } => Ok(StoredChunkRef::Stored {
                hash,
                data: Arc::new(data),
            }),
            OwnedHashTreeNode::Parent {
                hash, left, right, ..
            } => Ok(StoredChunkRef::Parent {
                hash,
                left: Arc::new(StoredChunkRef::try_from(*left)?),
                right: Arc::new(StoredChunkRef::try_from(*right)?),
            }),
            skipped @ OwnedHashTreeNode::Skipped { .. } => Err(skipped),
        }
    }
}

impl StoredChunkRef {
    #[must_use]
    #[inline(always)]
    pub fn hash(&self) -> &Hash {
        match self {
            Self::Stored { hash, .. } | Self::Parent { hash, .. } => hash,
        }
    }

    /// Compute sum size in bytes of all descending chunks
    #[must_use]
    #[inline(always)]
    pub fn size(&self) -> usize {
        match self {
            Self::Stored { data, .. } => data.len(),
            Self::Parent { left, right, .. } => left.size() + right.size(),
        }
    }

    #[must_use]
    pub fn chunk_info(&self) -> ChunkInfo {
        match self {
            Self::Stored { hash, data } => ChunkInfo {
                hash: *hash,
                size: data.len() as u32,
            },
            Self::Parent { hash, left, right } => ChunkInfo {
                hash: *hash,
                size: (left.size() + right.size()) as u32,
            },
        }
    }

    /// Get contained data, returns None if is not Stored
    #[must_use]
    pub fn stored_data(&self) -> Option<ArcChunk> {
        match self {
            Self::Stored { data, .. } => Some(data.clone()),
            Self::Parent { .. } => None,
        }
    }

    /// Get contained data, returns None if is not Parent
    #[must_use]
    pub fn children(&self) -> Option<(&Arc<StoredChunkRef>, &Arc<StoredChunkRef>)> {
        match self {
            Self::Parent { left, right, .. } => Some((left, right)),
            Self::Stored { .. } => None,
        }
    }

    /// Get a view on contained data, recursing across all children
    #[must_use]
    pub fn data(&self) -> Option<Vec<ArcChunk>> {
        match self {
            Self::Stored { data, .. } => Some(vec![data.clone()]),
            Self::Parent { left, right, .. } => {
                let mut left_vec = left.data()?;
                left_vec.extend(right.data()?);
                Some(left_vec)
            }
        }
    }

    /// Get contained data, recursing across all children
    /// This method may be slow and produce (copying) a large result, pay attention when using it
    #[must_use]
    pub fn clone_data(&self) -> Vec<u8> {
        match self {
            Self::Stored { data, .. } => (*data.clone()).clone(),
            Self::Parent { left, right, .. } => {
                let mut left_vec = left.clone_data();
                left_vec.extend(right.clone_data());
                left_vec
            }
        }
    }

    /// Get flatten representation of `Stored` hashes, eventually repeating hashes
    #[must_use]
    pub fn flatten(&self) -> Vec<Hash> {
        match self {
            Self::Stored { hash, .. } => {
                vec![*hash]
            }
            Self::Parent { left, right, .. } => {
                let mut left_vec = left.flatten();
                left_vec.extend(right.flatten());
                left_vec
            }
        }
    }

    /// Get all unique `Stored` hashes referenced by the (sub-)tree
    #[must_use]
    pub fn hashes(&self) -> HashSet<Hash> {
        match self {
            Self::Stored { hash, .. } => HashSet::from([*hash]),
            Self::Parent { left, right, .. } => {
                let left_vec = left.hashes();
                left_vec.union(&right.hashes()).copied().collect()
            }
        }
    }

    /// Get all unique hashes (`Stored` or `Parent`) referenced by the (sub-)tree
    #[must_use]
    pub fn all_hashes(&self) -> HashSet<Hash> {
        match self {
            Self::Stored { hash, .. } => HashSet::from([*hash]),
            Self::Parent { hash, left, right } => {
                let mut left_vec = left.all_hashes();
                left_vec.insert(*hash);
                left_vec.union(&right.all_hashes()).copied().collect()
            }
        }
    }

    /// Get all unique `Stored` hashes referenced by the (sub-)tree
    #[must_use]
    pub fn hashes_with_sizes(&self) -> HashSet<ChunkInfo> {
        match self {
            Self::Stored { hash, .. } => HashSet::from([ChunkInfo {
                size: self.size() as u32,
                hash: *hash,
            }]),
            Self::Parent { left, right, .. } => {
                let left_vec = left.hashes_with_sizes();
                left_vec
                    .union(&right.hashes_with_sizes())
                    .copied()
                    .collect()
            }
        }
    }

    /// Get all unique `Stored` hashes referenced by the (sub-)tree
    #[must_use]
    pub fn all_hashes_with_sizes(&self) -> HashSet<ChunkInfo> {
        match self {
            Self::Stored { hash, .. } => HashSet::from([ChunkInfo {
                size: self.size() as u32,
                hash: *hash,
            }]),
            Self::Parent { hash, left, right } => {
                let mut left_vec = left.hashes_with_sizes();
                left_vec.insert(ChunkInfo {
                    size: self.size() as u32,
                    hash: *hash,
                });
                left_vec
                    .union(&right.hashes_with_sizes())
                    .copied()
                    .collect()
            }
        }
    }

    /// Get flatten representation of `Stored` hashes with sizes, eventually repeating hashes
    #[must_use]
    pub fn flatten_with_sizes(&self) -> Vec<ChunkInfo> {
        match self {
            Self::Stored { hash, .. } => {
                vec![ChunkInfo {
                    size: self.size() as u32,
                    hash: *hash,
                }]
            }
            Self::Parent { left, right, .. } => {
                let mut left_vec = left.flatten_with_sizes();
                left_vec.extend(right.flatten_with_sizes());
                left_vec
            }
        }
    }

    /// Get diff sub-tree: required tree to reconstruct current node if one has the `hashes`
    #[must_use]
    pub fn find_diff(&self, hashes: &[Hash]) -> OwnedHashTreeNode {
        match self {
            Self::Parent { hash, left, right } => {
                // Go down and recursively find diffs
                let left = left.find_diff(hashes);
                let right = right.find_diff(hashes);

                let size = left.size() + right.size();

                // When coming back up skip whole sub-trees if both children are skipped
                match (&left, &right) {
                    (OwnedHashTreeNode::Skipped { .. }, OwnedHashTreeNode::Skipped { .. }) => {
                        OwnedHashTreeNode::Skipped { hash: *hash, size }
                    }
                    _ => OwnedHashTreeNode::Parent {
                        size,
                        hash: *hash,
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                }
            }
            Self::Stored { hash, .. } if hashes.contains(hash) => OwnedHashTreeNode::Skipped {
                hash: *self.hash(),
                size: self.size() as u32,
            },
            node @ Self::Stored { .. } => OwnedHashTreeNode::from(node.clone()),
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
    #[must_use]
    pub fn flatten_iter(&self) -> Box<dyn Iterator<Item = Arc<Vec<u8>>>> {
        match self {
            Self::Stored { data, .. } => Box::new([data.clone()].into_iter()),
            Self::Parent { left, right, .. } => {
                Box::new(left.flatten_iter().chain(right.flatten_iter()))
            }
        }
    }
}
