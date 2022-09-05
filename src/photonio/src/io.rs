pub use std::io::Result;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub trait SequentialRead {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>>;
}

pub trait SequentialWrite {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>>;
}

pub trait SequentialWriteExt: SequentialWrite {
    fn write_all(&self, buf: &[u8]) -> WriteAll {
        WriteAll {}
    }
}

impl<T: SequentialWrite + ?Sized> SequentialWriteExt for T {}

pub struct WriteAll {}

impl Future for WriteAll {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        todo!()
    }
}

pub trait PositionalRead {
    fn poll_read_at(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
        offset: u64,
    ) -> Poll<Result<usize>>;
}

pub trait PositionalReadExt: PositionalRead {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> ReadAt {
        ReadAt {}
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> ReadExactAt {
        ReadExactAt {}
    }
}

impl<T: PositionalRead + ?Sized> PositionalReadExt for T {}

pub struct ReadAt {}

impl Future for ReadAt {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        todo!()
    }
}

pub struct ReadExactAt {}

impl Future for ReadExactAt {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        todo!()
    }
}

pub trait PositionalWrite {
    fn poll_write_at(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
        offset: u64,
    ) -> Poll<Result<usize>>;
}
