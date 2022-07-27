use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Out of memory")]
    NoMem,
    #[error("Conflict")]
    Conflict,
    #[error("Corrupted: {0}")]
    Corrupted(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
