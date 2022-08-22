use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use crate::io::{PositionalRead, PositionalWrite, Result, SequentialRead, SequentialWrite};

pub struct Env;

impl Env {
    pub async fn open_sequential_file<P: AsRef<Path>>(&self, path: P) -> Result<SequentialFile> {
        let file = File::open(path)?;
        Ok(SequentialFile { file })
    }

    #[cfg(unix)]
    pub async fn open_positional_file<P: AsRef<Path>>(&self, path: P) -> Result<PositionalFile> {
        let file = File::open(path)?;
        Ok(PositionalFile { file })
    }
}

pub struct SequentialFile {
    file: File,
}

impl SequentialRead for SequentialFile {
    fn poll_read(mut self: Pin<&mut Self>, _: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        Poll::Ready(self.file.read(buf))
    }
}

impl SequentialWrite for SequentialFile {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        Poll::Ready(self.file.write(buf))
    }
}

#[cfg(unix)]
pub struct PositionalFile {
    file: File,
}

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(unix)]
impl PositionalRead for PositionalFile {
    fn poll_read_at(
        self: Pin<&mut Self>,
        _: &mut Context,
        buf: &mut [u8],
        offset: u64,
    ) -> Poll<Result<usize>> {
        Poll::Ready(self.file.read_at(buf, offset))
    }
}

#[cfg(unix)]
impl PositionalWrite for PositionalFile {
    fn poll_write_at(
        self: Pin<&mut Self>,
        _: &mut Context,
        buf: &[u8],
        offset: u64,
    ) -> Poll<Result<usize>> {
        Poll::Ready(self.file.write_at(buf, offset))
    }
}
