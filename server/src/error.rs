use thiserror::Error;
use distd_core::chunk_storage::StorageError;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Cannot insert chunk in data store")]
    ChunkInsertError(#[from] std::io::Error),
    #[error("Cannot insert key `{0}` as item name, already taken")]
    ItemInsertionError(String),
    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader {
        expected: String,
        found: String,
    },
    #[error("Cannot acquire lock")]
    LockError,
    #[error("unknown data store error")]
    UnknownDataStore,
    #[error("unknown server error")]
    Unknown,
}