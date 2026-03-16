use std::str::Utf8Error;

use thiserror::Error;

use crate::{chunk_storage::StorageError, hash::Hash};

/// Generic `distd_core` error
#[derive(Error, Debug)]
pub enum Error {
    #[error("Generic IO error")]
    IoError(#[from] std::io::Error),

    #[error("Missing data")]
    MissingData,

    #[error("Incomplete tree: missing subtree for hash {0}")]
    IncompleteTree(Hash),

    #[error("{0}")]
    Other(String),

    #[error("Invalid parameter: '{0}'")]
    InvalidParameter(#[from] InvalidParameter),

    #[error("Communication error: '{0}'")]
    Communication(#[from] Communication),

    #[error("Storage error: '{0}'")]
    Storage(#[from] StorageError),
}

/// Invalid parameter error
#[derive(Error, Debug)]
pub enum InvalidParameter {
    #[error("Invalid BLAKE3 hash")]
    Hash(#[from] crate::hash::HexError),

    #[error("Invalid parameter: expected {expected}, got \"{got}\"")]
    Generic { expected: String, got: String },

    #[error("Invalid URI")]
    Uri(#[from] http::uri::InvalidUri),

    #[error("Invalid bitcode")]
    Bitcode(#[from] bitcode::Error),

    #[error("Cannot decode UTF-8 string")]
    Utf8(#[from] Utf8Error),

    #[error("Parameter missing: '{0}'")]
    Missing(String),

    #[error("Invalid UUID")]
    Uuid(#[from] uuid::Error),

    #[error("Integrer conversion error")]
    IntError(#[from] std::num::TryFromIntError),
}

/// Communication error
#[derive(Error, Debug)]
pub enum Communication {
    #[error("Cannot read response")]
    ReadFromResponse(#[from] std::io::Error),

    #[error("Cannot reconstruct buffer from response")]
    ResponseDeserialize(#[from] bitcode::Error),

    #[error("Invalid parameter")]
    InvalidParameter(#[from] InvalidParameter),

    #[error("Invalid format for provided public key")]
    BadPubKey,
}
