use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Again")]
    Again,
    #[error("Alloc")]
    Alloc,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
