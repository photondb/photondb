use std::{io::Result, path::Path};

use async_trait::async_trait;
use photonio::io::{PositionalRead, SequentialWrite};

#[async_trait]
pub trait Env {
    type SequentialFile: SequentialWrite;
    type PositionalFile: PositionalRead;

    async fn open_sequential_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::SequentialFile>;

    async fn open_positional_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::PositionalFile>;
}
