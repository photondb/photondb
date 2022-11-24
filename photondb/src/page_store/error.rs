use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("Again")]
    Again,
    #[error("Corrupted")]
    Corrupted,
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("Memory Limit")]
    #[allow(dead_code)]
    MemoryLimit,
    #[error("IO {0}")]
    Io(#[from] std::io::Error),
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
