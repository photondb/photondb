use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {}

pub type Result<T> = std::result::Result<T, Error>;
