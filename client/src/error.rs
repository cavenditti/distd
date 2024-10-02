use std::str::Utf8Error;

use config::ConfigError;
use distd_core::{
    error::InvalidParameter, GrpcError, TransportError,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerConnection {
    #[error("Cannot create stream")]
    StreamCreation(#[from] std::io::Error),

    #[error("Invalid parameter")]
    InvalidParmeter(#[from] InvalidParameter),
}

#[derive(Error, Debug)]
pub enum ServerRequest {
    #[error("Cannot read response")]
    ReadFromResponse(#[from] std::io::Error),

    #[error("Cannot reconstruct buffer from server response")]
    ResponseDeserialize(#[from] bitcode::Error),

    #[error("gRPC error")]
    Grpc(#[from] GrpcError),

    #[error("gRPC transport error, is server accepting connetions?")]
    Transport(#[from] TransportError),

    #[error("Cannot decode UTF-8 string from server response")]
    Utf8(#[from] Utf8Error),

    #[error("Server didn't return a Uuid")]
    MissingUuid,

    #[error("Server didn't return a valid Uuid")]
    BadUuid,

    #[error("Cannot decode UTF-8 string from server response")]
    Uuid(#[from] uuid::Error),

    #[error("Invalid parameter")]
    InvalidParmeter(#[from] InvalidParameter),

    #[error("Cannot connect to server")]
    Connection(#[from] ServerConnection),

    #[error("Invalid format for provided server public key")]
    BadPubKey,
}

#[derive(Error, Debug)]
pub enum Client {
    #[error("No command specified")]
    MissingCmd,

    #[error("Invalid command provided: \"{0}\"")]
    InvalidCmd(String),

    #[error("Invalid args provided: {0:?}")]
    InvalidArgs(Vec<String>),

    #[error("Invalid config")]
    InvaldConfig(#[from] ConfigError),

    #[error("Cannot connect to server")]
    ServerConnection(#[from] ServerConnection),

    #[error("Cannot complete server request")]
    ServerRequest(#[from] ServerRequest),

    #[error("Requested file could not be found on server: \"{0}\"")]
    FileNotFound(String),

    #[error("Cannot insert item \"{0}\" into storage")]
    ItemInsertion(String),

    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[error("Generic storage error")]
    Storage,

    #[error("Cannot reconstruct tree from storage")]
    TreeReconstruct,

    #[error("Invalid parameter")]
    InvalidParmeter(#[from] InvalidParameter),

    #[error("User terminated")]
    Terminated,

    #[error("User specified item doesn't exist on server")]
    MissingItem,

    #[error("Error reported from core: '{0}'")]
    Core(#[from] distd_core::error::Error),
}
