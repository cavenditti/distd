//! distd metadata format
//!
//! This is the reference strucure in JSON.
//! This is very similar to a torrent/metainfo file. Some differences:
//!   - 1. We're ignoring everything tracker related, as we already know the server and it does most of what of tracker
//!       shoud do
//!   - 2. Uses BLAKE3 instead of SHA1 or SHA25
//!   - 3. No info_hash, no encoding. We'll use a binary format and the server will sign it
//!   - 4. We're calling them "chunks" instead of "pieces", because I like it more this way
//!   - 5. An item contains a single file. Msgpack serialization is cheap. Just use tar if you need to :)
//!{
//! "name": "Update for some file",
//! "description": "Description field, a string to put whatever you like",
//! "path": "relative/path/for/file.ext",
//! "revision": "2"
//! "created": 1375363666,
//! "created_by": "distd 0.1.0",
//! "format": "1"
//! "chunk_size": 16384,
//! "chunks": [
//!   "8a468d4f30b20645981364d3b77499f0d3dc999d25960cdfc5da8e836ce51b9d",
//!   "36875eae0dba363968a1e2f12d6be4aff5d737d0cca2d12351ccf182531a8613",
//!   ...
//!  ]
//! "signature": <build-key signature of file> ???
//!}

use blake3::Hash;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
//use ring::signature::Signature;

use serde::{Deserialize, Serialize};

use crate::utils::serde::{
    deserialize_hash, deserialize_hash_vec, serialize_hash, serialize_hash_vec,
};
use crate::{chunk_storage::ChunkStorage, msgpack::MsgPackSerializable, unique_name::UniqueName};

pub const CHUNK_SIZE: usize = 4 * 1024;

//pub type RawChunk = [u8; CHUNK_SIZE];
pub type RawChunk = Arc<Vec<u8>>;
pub type RawHash = [u8; 32];
pub type ItemName = UniqueName;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ItemFormat {
    V1 = 1,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

pub struct ChunksPack {
    chunk_size: usize,
    last_chunk_size: usize,
    hashes: Vec<Hash>, // We only keep hashes for chunks, they will then be retrieved from storage
}
//pub type ChunksMap = BTreeMap<u64, ChunkInfo>;
pub type ChunksMap = HashMap<PathBuf, ChunksPack>;

/// Item representation
///
/// This is bothe the format used over-the-wire to communicate from client to server, as well as the internal format
/// used by both server and client/peers.
///
/// We're assuming this is produced by a non-ill-intended trusted party, and we're not permorming many checks (e.g. on
/// name and descprition length).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Item {
    /// Name of the Item
    pub name: ItemName,
    /// Optional description, a generic String
    pub description: Option<String>,
    /// Incremental number of the file revision
    pub revision: u32,
    /// Path of the file (it may change among revisions?)
    pub path: PathBuf,
    // /// Size in bytes of each chunk
    //pub chunk_size: usize,
    /// BLAKE3 hashes of the chunks
    pub chunks: Vec<ChunkInfo>,
    // Should be something like this: allowing for content-addressable chunking
    //pub chunks: HashMap<PathBuf, (usize, usize, Vec<Hash>)>,
    /// Creation SystemTime
    pub created: SystemTime,
    /// Version used to create the Item (same as the output of env!("CARGO_PKG_VERSION") on the creator.
    pub created_by: String,
    /// format used
    pub format: ItemFormat,
    //signature: Signature,
}

impl Item {
    pub fn new<T: ChunkStorage + Clone>(
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        file: Bytes,
        storage: T,
    ) -> Result<Self, std::io::Error> {
        let mut chunks = Vec::new();
        for chunk in file.chunks(CHUNK_SIZE) {
            if let Some(res) = storage.insert(chunk) {
                chunks.push(res);
            } else {
                return Err(std::io::Error::other("Cannot insert chunk"));
            }
        }

        Ok(Self {
            name,
            description,
            revision,
            path,
            //chunk_size: CHUNK_SIZE,
            chunks,
            created: SystemTime::now(),
            created_by: env!("CARGO_PKG_VERSION").to_owned(),
            format: ItemFormat::V1,
        })
    }
}

impl<'a> MsgPackSerializable<'a, Item> for Item {}

#[cfg(test)]
mod tests {
    #[test]
    fn test_item_new() {
        assert!(true)
    }
}
