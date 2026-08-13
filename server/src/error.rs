use distd_core::error::InvalidParameter;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Server {
    #[error("server I/O error")]
    Io(#[from] std::io::Error),

    #[error("durable server state error: {0}")]
    DurableState(#[from] distd_core::durable_state::DurableStateError),

    #[error("Cannot insert chunk in data store")]
    ChunkInsertError,

    //ChunkInsertError(#[from] std::io::Error),
    #[error("Cannot insert item into storage")]
    ItemInsertionError,

    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader { expected: String, found: String },

    #[error("Invalid parameter: '{0}'")]
    InvalidParameter(#[from] InvalidParameter),

    #[error("QUIC transport error: {0}")]
    Quic(String),

    #[error("server state error: {0}")]
    State(String),

    #[error("unknown data store error")]
    UnknownDataStore,

    #[error("unknown server error")]
    Unknown,
}
