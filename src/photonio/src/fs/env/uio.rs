use std::os::unix::fs::RawFd;

use io_uring::{IoUring, opcode};

use crate::io::{FileIo, SequentialRead, Result, SequentialWrite, PositionalRead, PositionalWrite};

#[derive(Clone)]
struct Io {
    inner: Arc<IoUring>,
}

pub struct Env {
    io: IoUring,
}

impl Env where {
    pub async fn open_sequential_file<P: AsRef<Path>>(&self, path: P) -> Result<SequentialFile> {
        let file = self.io.open(path)?;
        Ok(SequentialFile { file })
    }
}

impl Env where {
    pub async fn open_positional_file<P: AsRef<Path>>(&self, path: P) -> Result<PositionalFile> {
        let file = self.io.open(path)?;
        Ok(PositionalFile { file })
    }
}

pub struct SequentialFile<Io> {
    fd: RawFd,
    io: IoUring,
}

impl SequentialRead for SequentialFile {
    fn poll_read(self: Pin<&mut Self>, _: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let ent = opcode::Read::new(self.fd, buf.as_mut_ptr(), buf.len() as u32);
    }
}

pub struct PositionalFile {
    fd: RawFd,
    io: IoUring,
}

impl PositionalRead for PositionalFile {
    fn poll_read_at(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8], offset: u64) -> Poll<Result<usize>> {
        let ent = opcode::Read::new(self.fd, buf.as_mut_ptr(), buf.len() as u32).offset(offset);
    }
}