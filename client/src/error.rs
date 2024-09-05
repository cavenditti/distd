use distd_core::chunks::HashTreeNodeTypeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("No command specified")]
    MissingCmd,

    #[error("Invalid args provided")]
    InvalidArgs,

    #[error("Config file not found: {0}")]
    MissingConfig(String),

    #[error("Invalid config: {0}")]
    InvaldConfig(String),

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

    #[error("Cannot insert item into storage")]
    IoError(String),

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
