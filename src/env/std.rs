use std::{fs::File, future::Future, io::Result, path::Path, thread};

use futures::executor::block_on;

use super::{ReadAt, Write};

/// An implementation of [`super::Env`] based on [`std`].
pub struct Env;

#[super::async_trait]
impl super::Env for Env {
    type PositionalReader = PositionalReader;
    type SequentialWriter = SequentialWriter;

    async fn open_positional_reader<P>(&self, path: P) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send,
    {
        let file = File::open(path)?;
        Ok(PositionalReader(file))
    }

    async fn open_sequential_writer<P>(&self, path: P) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send,
    {
        let file = File::create(path)?;
        Ok(SequentialWriter(file))
    }

    async fn spawn_background<F>(&self, f: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let handle = thread::spawn(move || block_on(f));
        handle.join().unwrap()
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
