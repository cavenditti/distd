use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Generic IO error")]
    IoError(#[from] std::io::Error),

    #[error("Missing data")]
    MissingData,

    #[error("{0}")]
    Other(String),
}
