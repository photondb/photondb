pub use std::io::Result;
use std::{
    os::unix::io::RawFd,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

pub trait FileIo {
    fn poll_open<P: AsRef<Path>>(&self, cx: &mut Context, path: P) -> Poll<Result<RawFd>>;
}

pub trait SocketIo {
    fn poll_accept(&self, cx: &mut Context) -> Poll<Result<()>>;

    fn poll_connect(&self, cx: &mut Context) -> Poll<Result<()>>;
}

pub trait SequentialIo {
    fn poll_read(&self, cx: &mut Context, fd: RawFd, buf: &mut [u8]) -> Poll<Result<usize>>;

    fn poll_write(&self, cx: &mut Context, fd: RawFd, buf: &[u8]) -> Poll<Result<usize>>;
}

pub trait SequentialRead {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>>;
}

pub trait SequentialWrite {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>>;
}

pub trait PositionalIo {
    fn poll_read_at(
        &self,
        cx: &mut Context,
        fd: RawFd,
        buf: &mut [u8],
        offset: u64,
    ) -> Poll<Result<usize>>;

    fn poll_write_at(
        &self,
        cx: &mut Context,
        fd: RawFd,
        buf: &[u8],
        offset: u64,
    ) -> Poll<Result<usize>>;
}

pub trait PositionalRead {
    fn poll_read_at(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
        offset: u64,
    ) -> Poll<Result<usize>>;
}

pub trait PositionalWrite {
    fn poll_write_at(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
        offset: u64,
    ) -> Poll<Result<usize>>;
}
