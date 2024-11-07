use std::str::Utf8Error;

use thiserror::Error;
use tonic::metadata::errors::{InvalidMetadataValue, InvalidMetadataValueBytes};

use crate::{chunk_storage::StorageError, GrpcError, TransportError};

/// Generic `distd_core` error
#[derive(Error, Debug)]
pub enum Error {
    #[error("Generic IO error")]
    IoError(#[from] std::io::Error),

    #[error("Missing data")]
    MissingData,

    #[error("{0}")]
    Other(String),

    #[error("Invalid parameter: '{0}'")]
    InvalidParmeter(#[from] InvalidParameter),

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

    #[error("Invalid metadata, possibly a bug in code")]
    Metadata(#[from] InvalidMetadataValue),

    #[error("Invalid binary metadata, possibly a bug in code")]
    MetadataBytes(#[from] InvalidMetadataValueBytes),

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

    #[error("gRPC error")]
    Grpc(#[from] GrpcError),

    #[error("gRPC transport error, is server accepting connetions?")]
    Transport(#[from] TransportError),

    #[error("Invalid parameter")]
    InvalidParmeter(#[from] InvalidParameter),

    #[error("Invalid format for provided public key")]
    BadPubKey,
}
