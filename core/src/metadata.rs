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

use std::io::Read;
use std::fs::File;
use std::time::Instant;
//use ring::signature::Signature;

use blake3::Hash;

pub const CHUNK_SIZE: usize = 256 * 1024;

pub type RawChunk = [u8; CHUNK_SIZE];

pub type ChunksPack = Vec<Hash>; // We only keep hashes for chunks, they will then be retrieved from storage

#[derive(Debug)]
pub enum ItemFormat{
    V1 = 1
}

/// Item representation
///
/// This is bothe the format used over-the-wire to communicate from client to server, as well as the internal format
/// used by both server and client/peers.
///
/// We're assuming this is produced by a non-ill-intended trusted party, and we're not permorming many checks (e.g. on
/// name and descprition length).
#[derive(Debug)]
pub struct Item {
    /// Name of the Item
    name: String,
    /// Optional description, a generic String
    description: Option<String>,
    /// Incremental number of the file revision
    revision: u32,
    /// Size in bytes of each chunk
    chunk_size: usize,
    /// BLAKE3 hashes of the chunks
    chunks: ChunksPack,
    /// Creation Instant
    created: Instant,
    /// Version used to create the Item (same as the output of env!("CARGO_PKG_VERSION") on the creator.
    created_by: String,
    /// format used
    format: ItemFormat,
    //signature: Signature,
}

impl Item {
    pub fn new(
        name: String,
        description: Option<String>,
        revision: u32,
        file: &mut File,
    ) -> Result<Self, std::io::Error> {
        let mut chunks = ChunksPack::new();
        loop {
            let mut raw_chunk: RawChunk = [0; CHUNK_SIZE];
            let n = file.by_ref().take(CHUNK_SIZE as u64).read(&mut raw_chunk)?;
            if n == 0 {
                break;
            }
            chunks.push(blake3::hash(&raw_chunk));
            // push chunk to storage
            if n < CHUNK_SIZE {
                break;
            }
        }
        Ok(Self {
            name,
            description,
            revision,
            chunk_size: CHUNK_SIZE,
            chunks,
            created: Instant::now(),
            created_by: env!("CARGO_PKG_VERSION").to_owned(),
            format: ItemFormat::V1,
        })
    }
}

#[cfg(test)]
mod tests {
    fn test_item_new() {}
}
