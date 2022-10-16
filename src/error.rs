use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Again")]
    Again,
    #[error("Corrupted")]
    Corrupted,
}

pub type Result<T> = std::result::Result<T, Error>;
