use thiserror::Error;

use crate::page_store::Error as PageError;

/// A list of possible errors returned by public APIs.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Corrupted")]
    Corrupted,
    #[error("Unknown")]
    Unknown,
}

impl From<PageError> for Error {
    fn from(err: PageError) -> Self {
        match err {
            PageError::Corrupted => Self::Corrupted,
            _ => Self::Unknown,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
