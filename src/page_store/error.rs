use thiserror::Error;

use super::PageAddr;

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("Again")]
    Again,
    #[error("Corrupted")]
    Corrupted,
    #[error("UpdatePage")]
    UpdatePage(PageAddr),
}

pub(crate) type Result<T> = std::result::Result<T, Error>;
