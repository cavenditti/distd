use std::str::Utf8Error;

use config::ConfigError;
use distd_core::{
    error::InvalidParameter,
    transport::{PayloadCompressionError, WireCompatibilityError},
};
use thiserror::Error;

use crate::{
    persistence::PersistenceError,
    publisher::{PublisherAuthenticityError, PublisherConfigError},
    rollback::RollbackError,
    server_url::ServerUrlError,
};

#[derive(Error, Debug)]
pub enum ServerConnection {
    #[error("Cannot create stream")]
    StreamCreation(#[from] std::io::Error),

    #[error("QUIC connection error: {0}")]
    Quic(String),

    #[error("Invalid parameter")]
    InvalidParameter(#[from] InvalidParameter),
}

#[derive(Error, Debug)]
pub enum ServerRequest {
    #[error("Cannot read response")]
    ReadFromResponse(#[from] std::io::Error),

    #[error("Cannot reconstruct buffer from server response")]
    ResponseDeserialize(#[from] bitcode::Error),

    #[error("Cannot decode UTF-8 string from server response")]
    Utf8(#[from] Utf8Error),

    #[error("Server didn't return a Uuid")]
    MissingUuid,

    #[error("Server didn't return a valid Uuid")]
    BadUuid,

    #[error("server returned a different client identity for this authority")]
    ClientIdentityChanged,

    #[error("Cannot decode UTF-8 string from server response")]
    Uuid(#[from] uuid::Error),

    #[error("Invalid parameter")]
    InvalidParameter(#[from] InvalidParameter),

    #[error("Cannot connect to server")]
    Connection(#[from] ServerConnection),

    #[error("Invalid format for provided server public key")]
    BadPubKey,

    #[error("Sync stream closed unexpectedly")]
    StreamClosed,

    #[error("Unexpected message in sync protocol")]
    UnexpectedMessage,

    #[error("Invalid hash in sync response")]
    BadHash,

    #[error("QUIC transport error: {0}")]
    Quic(String),

    #[error("server wire contract is incompatible: {0}")]
    WireCompatibility(#[from] WireCompatibilityError),

    #[error("Invalid compressed sync payload")]
    Compression(#[from] PayloadCompressionError),

    #[error("publisher authenticity validation failed: {0}")]
    PublisherAuthenticity(#[from] PublisherAuthenticityError),

    #[error("publisher verification worker failed")]
    PublisherVerificationWorker,

    #[error("publisher rollback protection failed: {0}")]
    RollbackProtection(#[from] RollbackError),

    #[error("server metadata worker failed")]
    MetadataWorker,
}

#[derive(Error, Debug)]
pub enum Client {
    #[error("server certificate is not pinned; configure server.certificate_fingerprint")]
    UnpinnedServer,

    #[error("configured server certificate conflicts with durable trust state")]
    TrustConflict,

    #[error("invalid server URL")]
    ServerUrl(#[from] ServerUrlError),

    #[error("cannot load or store client trust state")]
    Persistence(#[from] PersistenceError),

    #[error("client trust persistence worker failed")]
    PersistenceWorker(#[from] tokio::task::JoinError),

    #[error("client storage worker failed")]
    StorageWorker,

    #[error("No command specified")]
    MissingCmd,

    #[error("Invalid command provided: \"{0}\"")]
    InvalidCmd(String),

    #[error("Invalid args provided: {0:?}")]
    InvalidArgs(Vec<String>),

    #[error("Invalid config")]
    InvaldConfig(#[from] ConfigError),

    #[error("invalid publisher trust configuration: {0}")]
    PublisherConfiguration(#[from] PublisherConfigError),

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
    InvalidParameter(#[from] InvalidParameter),

    #[error("User terminated")]
    Terminated,

    #[error("User specified item doesn't exist on server")]
    MissingItem,

    #[error("Error reported from core: '{0}'")]
    Core(#[from] distd_core::error::Error),
}
