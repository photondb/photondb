use crate::io::{PositionalRead, PositionalWrite, SequentialRead, SequentialWrite};

pub mod env;

pub trait SequentialFile: SequentialRead + SequentialWrite {}

pub trait PositionalFile: PositionalRead + PositionalWrite {}
