use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Cannot insert chunk in data store")]
    ChunkInsertError,

    //ChunkInsertError(#[from] std::io::Error),
    #[error("Cannot insert key `{0}` as item name, already taken")]
    ItemInsertionError(String),

    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader {
        expected: String,
        found: String,
    },

    #[error("unknown data store error")]
    UnknownDataStore,

    #[error("unknown server error")]
    Unknown,
}
