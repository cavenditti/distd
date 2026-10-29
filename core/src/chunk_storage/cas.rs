//! Content-Addressed Storage (CAS) for chunks.
//!
//! Stores chunks as `$root/<2-char-prefix>/<full-hash>` files on disk.
//! This provides natural deduplication and resume capability.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::hash::Hash;

/// A simple content-addressed chunk store backed by the filesystem.
#[derive(Debug, Clone)]
pub struct CasStorage {
    root: PathBuf,
}

impl CasStorage {
    /// Create or open a CAS store at the given root directory.
    pub fn new(root: PathBuf) -> Self {
        fs::create_dir_all(&root).expect("create CAS root");
        Self { root }
    }

    /// Default CAS location under the user's cache directory.
    #[must_use]
    pub fn default_path() -> PathBuf {
        crate::utils::settings::cache_dir().join("cas")
    }

    fn chunk_path(&self, hash: &Hash) -> PathBuf {
        let hex = hash.to_blake3_hash().to_hex();
        let hex = hex.as_str();
        let prefix = &hex[..2];
        self.root.join(prefix).join(hex)
    }

    /// Store a chunk. No-op if already present.
    pub fn store(&self, hash: &Hash, data: &[u8]) -> std::io::Result<()> {
        let path = self.chunk_path(hash);
        if path.exists() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Use a per-call unique temp name so that two concurrent stores of the
        // same hash do not truncate each other's in-flight write (TOCTOU).
        let nonce = rand::random::<u64>();
        let tmp = path.with_extension(format!("{nonce:016x}.tmp"));
        let mut f = fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.flush()?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Retrieve a chunk by hash.
    pub fn get(&self, hash: &Hash) -> Option<Vec<u8>> {
        let path = self.chunk_path(hash);
        fs::read(&path).ok()
    }

    /// Check if a chunk exists.
    #[must_use]
    pub fn has(&self, hash: &Hash) -> bool {
        self.chunk_path(hash).exists()
    }

    /// Assemble chunks in order into a destination file.
    ///
    /// Reads each chunk from the CAS and writes them sequentially.
    pub fn assemble(&self, chunk_hashes: &[Hash], dest: &Path) -> std::io::Result<()> {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        // Write to staging then rename
        let staging = dest.with_extension("staging");
        {
            let mut f = fs::File::create(&staging)?;
            for hash in chunk_hashes {
                let data = self.get(hash).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Missing chunk {hash} in CAS"),
                    )
                })?;
                f.write_all(&data)?;
            }
            f.flush()?;
        }
        fs::rename(&staging, dest)?;
        Ok(())
    }
}

impl crate::blob::BlobChunkSource for CasStorage {
    fn read_chunk(
        &self,
        expected: &crate::chunks::ChunkInfo,
    ) -> std::io::Result<super::node::ArcChunk> {
        let mut file = fs::File::open(self.chunk_path(&expected.hash)).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                crate::blob::missing_chunk(expected)
            } else {
                error
            }
        })?;
        if file.metadata()?.len() != expected.size {
            return Err(crate::blob::invalid_chunk_size());
        }
        let size = usize::try_from(expected.size).map_err(|_| crate::blob::invalid_chunk_size())?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(std::io::Error::other)?;
        bytes.resize(size, 0);
        file.read_exact(&mut bytes)?;
        if file.read(&mut [0])? != 0 {
            return Err(crate::blob::invalid_chunk_size());
        }
        Ok(std::sync::Arc::new(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash;

    #[test]
    fn store_and_get() {
        let dir = std::env::temp_dir().join("distd_cas_test_store_get");
        let _ = fs::remove_dir_all(&dir);
        let cas = CasStorage::new(dir.clone());

        let data = b"hello world";
        let h = hash(data);
        cas.store(&h, data).unwrap();
        assert!(cas.has(&h));
        assert_eq!(cas.get(&h).unwrap(), data);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn assemble_chunks() {
        let dir = std::env::temp_dir().join("distd_cas_test_assemble");
        let _ = fs::remove_dir_all(&dir);
        let cas = CasStorage::new(dir.clone());

        let c1 = b"chunk one";
        let c2 = b"chunk two";
        let h1 = hash(c1);
        let h2 = hash(c2);
        cas.store(&h1, c1).unwrap();
        cas.store(&h2, c2).unwrap();

        let dest = dir.join("assembled.bin");
        cas.assemble(&[h1, h2], &dest).unwrap();

        let assembled = fs::read(&dest).unwrap();
        assert_eq!(&assembled[..c1.len()], c1);
        assert_eq!(&assembled[c1.len()..], c2);

        let _ = fs::remove_dir_all(&dir);
    }
}
