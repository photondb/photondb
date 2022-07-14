use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Aborted")]
    Aborted,
    #[error("Corrupted: {0}")]
    Corrupted(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> crate::Error {
        match err {
            Error::Aborted => unreachable!(),
            Error::Corrupted(msg) => crate::Error::Corrupted(msg),
            Error::Io(err) => crate::Error::Io(err),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
