use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

pub trait SequentialRead {
    fn poll_read(
        self: Pin<&mut Self>,
        ctx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>;

    fn poll_seek(self: Pin<&mut Self>, cx: &mut Context, pos: u64) -> Poll<io::Result<u64>>;
}

pub trait SequentialWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>;

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

pub trait RandomAcessRead {
    fn poll_read_at(
        self: Pin<&mut Self>,
        ctx: &mut Context,
        buf: &mut [u8],
        pos: u64,
    ) -> Poll<io::Result<usize>>;
}
