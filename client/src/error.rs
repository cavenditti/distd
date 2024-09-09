use config::ConfigError;
use distd_core::chunks::HashTreeNodeTypeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("No command specified")]
    MissingCmd,

    #[error("Invalid command provided: \"{0}\"")]
    InvalidCmd(String),

    #[error("Invalid args provided: {0:?}")]
    InvalidArgs(Vec<String>),

    #[error("Invalid config")]
    InvaldConfig(#[from] ConfigError),

    #[error("Cannot complete server request")]
    ServerRequestError,

    #[error("Cannot parse server response")]
    ServerResponseError,

    #[error("Cannot reconstruct buffer from server response")]
    ServerResponseReconstructError{
        #[from]
        source: HashTreeNodeTypeError,
    },

    #[error("Requested file could not be found on server: \"{0}\"")]
    FileNotFound(String),

    #[error("Cannot insert item \"{0}\" into storage")]
    ItemInsertion(String),

    #[error("IO error")]
    IoError(#[from] std::io::Error),

    /*
    #[error("Bad config specified: \"{0}\"")]
    InvaldConfig(String),

    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader {
        expected: String,
        found: String,
    },
    #[error("unknown data store error")]
    UnknownDataStore,
    #[error("unknown server error")]
    Unknown,
    */
}
