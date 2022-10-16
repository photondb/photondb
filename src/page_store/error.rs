use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("Again")]
    Again,
    #[error("Corrupted")]
    Corrupted,
    #[error("UpdatePage")]
    UpdatePage(u64),
}

pub(crate) type Result<T> = std::result::Result<T, Error>;
