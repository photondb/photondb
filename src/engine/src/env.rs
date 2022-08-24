use std::{
    io::Result,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
pub use photonio::io::{PositionalRead, SequentialRead, SequentialWrite};

#[async_trait]
pub trait Env {
    type ReadDir: ReadDir;
    type SequentialFile: SequentialWrite;
    type PositionalFile: PositionalRead;

    async fn make_dir<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    async fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<Self::ReadDir>;

    async fn path_exists<P: AsRef<Path>>(&self, path: P) -> Result<bool>;

    async fn lock_file<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    async fn unlock_file<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    async fn open_sequential_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::SequentialFile>;

    async fn open_positional_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::PositionalFile>;
}

pub trait ReadDir {
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Option<String>>>;
}
