use core::slice::SlicePattern;
use std::{slice::Chunks, sync::Arc};

use blake3::Hash;
use bytes::Bytes;
use serde::ser::{Serialize, SerializeStructVariant};

use crate::metadata::{ChunkInfo, RawChunk, CHUNK_SIZE};

pub mod hashmap_storage;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Unknown storage size")]
    UnknownSize,
    #[error("Cannot insert chunk in data store")]
    UnknownChunkInsertError(#[from] std::io::Error),
}

#[derive(Clone, Debug)]
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
        if serializer.is_human_readable() {
            match self {
                Self::Parent {
                    hash: _,
                    left,
                    right,
                } => {
                    let mut state =
                        serializer.serialize_struct_variant("StoredChunkRef", 0, "Parent", 2)?;
                    state.serialize_field("left", &left.get_hash().to_string())?;
                    state.serialize_field("right", &right.get_hash().to_string())?;
                    state.end()
                }
                Self::Stored { hash, data } => {
                    let mut state =
                        serializer.serialize_struct_variant("StoredChunkRef", 0, "Stored", 2)?;
                    state.serialize_field("hash", &hash.to_string())?;
                    state.serialize_field("data", &*data)?; // TODO do base64 encoding maybe?
                    state.end()
                }
            }
        } else {
            match self {
                Self::Parent {
                    hash,
                    left,
                    right,
                } => {
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

impl StoredChunkRef {
    pub fn get_hash(&self) -> &Hash {
        match self {
            Self::Stored { hash, data: _ } => &hash,
            Self::Parent {
                hash,
                left: _,
                right: _,
            } => hash,
        }
    }

    /// Compute sum size in bytes of all descending chunks
    pub fn get_size(&self) -> usize {
        match self {
            Self::Stored { hash: _, data } => data.len(),
            Self::Parent {
                hash: _,
                left,
                right,
            } => left.get_size() + right.get_size(),
        }
    }

    pub fn get_chunk_info(&self) -> ChunkInfo {
        match self {
            Self::Stored { hash, data } => ChunkInfo {
                hash: *hash,
                size: data.len() as u32,
            },
            Self::Parent { hash, left, right } => ChunkInfo {
                hash: *hash,
                size: (left.get_size() + right.get_size()) as u32,
            },
        }
    }

    /// Get contained data, returns None if is not Stored
    pub fn get_data(&self) -> Option<RawChunk> {
        match self {
            Self::Stored { hash: _, data } => Some(data.clone()),
            _ => None,
        }
    }

    pub fn flatten(&self) -> Vec<Hash> {
        match self {
            Self::Stored { hash, data: _ } => {
                vec![*hash]
            }
            Self::Parent {
                hash: _,
                left,
                right,
            } => {
                let mut left_vec = left.flatten();
                left_vec.extend(right.flatten());
                left_vec
            }
        }
    }

    pub fn flatten_with_sizes(&self) -> Vec<ChunkInfo> {
        match self {
            Self::Stored { hash, data: _ } => {
                vec![ChunkInfo {
                    size: self.get_size() as u32,
                    hash: *hash,
                }]
            }
            Self::Parent {
                hash: _,
                left,
                right,
            } => {
                let mut left_vec = left.flatten_with_sizes();
                left_vec.extend(right.flatten_with_sizes());
                left_vec
            }
        }
    }
}

/// Defines a backend used to store hashes and chunks ad key-value pairs
pub trait ChunkStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<StoredChunkRef>>;
    fn insert_chunk(&self, chunk: &[u8]) -> Option<Arc<StoredChunkRef>>;
    fn link(
        &self,
        left: Arc<StoredChunkRef>,
        right: Arc<StoredChunkRef>,
    ) -> Option<Arc<StoredChunkRef>>;
    fn chunks(&self) -> Vec<Hash>;
    /// Allocated size for all chunks, in bytes
    /// This only counts actual chunks size, excluding any auxiliary structure used by storage backend/adapter
    fn size(&self) -> usize;
    //fn drop(hash: Hash); // ??

    /// Insert bytes into the storage returning the associated hash tree
    fn insert(&self, data: Bytes) -> Option<Arc<StoredChunkRef>>
    where
        Self: Sized,
    {
        fn partial_tree(
            storage: &dyn ChunkStorage,
            left: &[&[u8]],
            right: &[&[u8]],
        ) -> Option<Arc<StoredChunkRef>> {
            match (left.len(), right.len()) {
                (1, 0) => storage.insert_chunk(left[0]),
                (0, 1) => storage.insert_chunk(right[0]),
                (_, 1) => storage.link(
                    partial_tree(storage, &left[..left.len() / 2], &left[..left.len() / 2])?,
                    partial_tree(storage, &[], right)?,
                ),
                (_, _) => storage.link(
                    partial_tree(storage, &left[..left.len() / 2], &left[..left.len() / 2])?,
                    partial_tree(
                        storage,
                        &right[..right.len() / 2],
                        &right[..right.len() / 2],
                    )?,
                ),
            }
        }

        let (chunks, remainder) = data.as_chunks::<CHUNK_SIZE>();
        partial_tree(
            self,
            chunks
                .iter()
                .map(|x| x.as_slice())
                .collect::<Vec<&[u8]>>() // FIXME is this zero copy?
                .as_slice(),
            &[remainder],
        )
    }
}
