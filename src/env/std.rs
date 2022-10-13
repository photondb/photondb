use std::{
    fs::File,
    future::Future,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
    thread,
};

use futures::executor::block_on;

use super::{ReadAt, Result, Write};

/// An implementation of [`super::Env`] based on [`std`].
pub struct Env;

impl super::Env for Env {
    type PositionalReader = PositionalReader;
    type OpenPositionalReader = impl Future<Output = Result<Self::PositionalReader>>;
    fn open_positional_reader(&self, path: &Path) -> Self::OpenPositionalReader {
        let file = File::open(path);
        async move { Ok(PositionalReader(file?)) }
    }

    type SequentialWriter = SequentialWriter;
    type OpenSequentialWriter = impl Future<Output = Result<Self::SequentialWriter>>;
    fn open_sequential_writer(&self, path: &Path) -> Self::OpenSequentialWriter {
        let file = File::create(path);
        async move { Ok(SequentialWriter(file?)) }
    }

    type JoinHandle = JoinHandle;
    fn spawn_background<F>(&self, f: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = thread::spawn(move || block_on(f));
        JoinHandle(handle)
    }
}

pub struct PositionalReader(File);

impl ReadAt for PositionalReader {
    type ReadAt<'a> = impl Future<Output = Result<usize>> + 'a;

    #[cfg(unix)]
    fn read_at<'a>(&'a self, buf: &'a mut [u8], offset: u64) -> Self::ReadAt<'a> {
        use std::os::unix::fs::FileExt;
        async move { self.0.read_at(buf, offset) }
    }
}

pub struct SequentialWriter(File);

impl Write for SequentialWriter {
    type Write<'a> = impl Future<Output = Result<usize>> + 'a;

    fn write<'a>(&'a mut self, buf: &'a [u8]) -> Self::Write<'a> {
        use std::io::Write as _;
        async move { self.0.write(buf) }
    }
}

pub struct JoinHandle(thread::JoinHandle<()>);

impl Future for JoinHandle {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}
