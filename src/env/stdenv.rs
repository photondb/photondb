use std::{fs::File, future::Future, io::Result, path::Path, thread};

use futures::{executor::block_on, future::BoxFuture};

use super::{async_trait, Env, ReadAt, Write};

/// An implementation of [`Env`] based on [`std`] with synchronous I/O.
#[derive(Clone)]
pub struct Std;

#[async_trait]
impl Env for Std {
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

    fn spawn_background<F>(&self, f: F) -> BoxFuture<'static, F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let handle = thread::spawn(move || block_on(f));
        Box::pin(async { handle.join().unwrap() })
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
