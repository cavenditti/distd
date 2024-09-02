use std::{collections::HashSet, sync::Arc};

use blake3::Hash;
use serde::ser::{Serialize, SerializeStructVariant};

use crate::chunks::{ChunkInfo, OwnedHashTreeNode, RawChunk};

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
        data: RawChunk,
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

impl StoredChunkRef {
    #[must_use] pub fn hash(&self) -> &Hash {
        match self {
            Self::Stored { hash, .. } | Self::Parent { hash, .. } => hash,
        }
    }

    /// Compute sum size in bytes of all descending chunks
    #[must_use] pub fn size(&self) -> usize {
        match self {
            Self::Stored { data, .. } => data.len(),
            Self::Parent { left, right, .. } => left.size() + right.size(),
        }
    }

    #[must_use] pub fn chunk_info(&self) -> ChunkInfo {
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
    #[must_use] pub fn stored_data(&self) -> Option<RawChunk> {
        match self {
            Self::Stored { data, .. } => Some(data.clone()),
            _ => None,
        }
    }

    /// Get contained data, returns None if is not Parent
    #[must_use] pub fn children(&self) -> Option<(&Arc<StoredChunkRef>, &Arc<StoredChunkRef>)> {
        match self {
            Self::Parent { left, right, .. } => Some((left, right)),
            _ => None,
        }
    }

    /// Get a view on contained data, recursing across all children
    #[must_use] pub fn data(&self) -> Option<Vec<RawChunk>> {
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
    #[must_use] pub fn clone_data(&self) -> Option<Vec<u8>> {
        match self {
            Self::Stored { data, .. } => Some((*data.clone()).clone()),
            Self::Parent { left, right, .. } => {
                let mut left_vec = left.clone_data()?;
                left_vec.extend(right.clone_data()?);
                Some(left_vec)
            }
        }
    }

    /// Get flatten representation of `Stored` hashes, eventually repeating hashes
    #[must_use] pub fn flatten(&self) -> Vec<Hash> {
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
    #[must_use] pub fn hashes(&self) -> HashSet<Hash> {
        match self {
            Self::Stored { hash, .. } => HashSet::from([*hash]),
            Self::Parent { left, right, .. } => {
                let left_vec = left.hashes();
                left_vec.union(&right.hashes()).copied().collect()
            }
        }
    }

    /// Get all unique hashes (`Stored` or `Parent`) referenced by the (sub-)tree
    #[must_use] pub fn all_hashes(&self) -> HashSet<Hash> {
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
    #[must_use] pub fn hashes_with_sizes(&self) -> HashSet<ChunkInfo> {
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
    #[must_use] pub fn all_hashes_with_sizes(&self) -> HashSet<ChunkInfo> {
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
    #[must_use] pub fn flatten_with_sizes(&self) -> Vec<ChunkInfo> {
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
}
