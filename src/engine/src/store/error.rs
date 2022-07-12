use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Corrupted: {0}")]
    Corrupted(String),
}

pub type Result<T> = std::result::Result<T, Error>;
