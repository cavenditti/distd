use thiserror::Error;
use tonic::metadata::errors::{InvalidMetadataValue, InvalidMetadataValueBytes};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Generic IO error")]
    IoError(#[from] std::io::Error),

    #[error("Missing data")]
    MissingData,

    #[error("{0}")]
    Other(String),

    #[error("Invalid parameter '{0}'")]
    InvalidParmeter(#[from] InvalidParameter),
}

#[derive(Error, Debug)]
pub enum InvalidParameter {
    #[error("Invalid BLAKE3 hash")]
    Hash(#[from] blake3::HexError),

    #[error("Invalid parameter: expected {expected}, got \"{got}\"")]
    Generic { expected: String, got: String },

    #[error("Invalid URI")]
    InvalidUri(#[from] http::uri::InvalidUri),

    #[error("Invalid metadata, possibly a bug in code")]
    InvalidMetadata(#[from] InvalidMetadataValue),

    #[error("Invalid binary metadata, possibly a bug in code")]
    InvalidMetadataBytes(#[from] InvalidMetadataValueBytes),
}
