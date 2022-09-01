pub use std::io::Result;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub trait SequentialRead {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>>;
}

pub trait SequentialWrite {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>>;
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
