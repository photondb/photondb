use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum Error {}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
