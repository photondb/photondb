use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Corrupted")]
    Corrupted,
    #[error("Conflicted")]
    Conflicted,
}

pub type Result<T> = std::result::Result<T, Error>;
