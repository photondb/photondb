use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::Result,
    os::fd::AsRawFd,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
    thread,
};

use futures::executor::block_on;

use super::*;

/// An implementation of [`Env`] based on [`std`] with synchronous I/O.
#[derive(Clone)]
pub struct Std;

#[async_trait]
impl Env for Std {
    type PositionalReader = PositionalReader;
    type SequentialWriter = SequentialWriter;
    type JoinHandle<T: Send> = JoinHandle<T>;

    async fn open_positional_reader<P>(&self, path: P) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send,
    {
        let file = OpenOptions::new().read(true).open(path.as_ref())?;
        Ok(PositionalReader(file))
    }

    async fn open_sequential_writer<P>(&self, path: P) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send,
    {
        Ok(SequentialWriter(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.as_ref())?,
        ))
    }

    fn spawn_background<F>(&self, f: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let handle = thread::spawn(move || block_on(f));
        JoinHandle {
            handle: Some(handle),
        }
    }

    /// An async version of [`std::fs::rename`].
    async fn rename<P: AsRef<Path> + Send, Q: AsRef<Path> + Send>(
        &self,
        from: P,
        to: Q,
    ) -> Result<()> {
        std::fs::rename(from, to)
    }

    /// An async version of [`std::fs::remove_file`].
    async fn remove_file<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        std::fs::remove_file(path)
    }

    /// An async version of [`std::fs::create_dir`].
    async fn create_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        std::fs::create_dir_all(path)
    }

    /// An async version of [`std::fs::remove_dir`].
    async fn remove_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        std::fs::remove_dir_all(path)
    }

    /// Returns an iterator over the entries within a directory.
    /// See alos [`std::fs::read_dir`].
    fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<std::fs::ReadDir> {
        std::fs::read_dir(path)
    }

    async fn metadata<P: AsRef<Path> + Send>(&self, path: P) -> Result<Metadata> {
        let raw_metadata = std::fs::metadata(path)?;
        Ok(Metadata {
            len: raw_metadata.len(),
            is_dir: raw_metadata.is_dir(),
        })
    }
}

pub struct PositionalReader(File);

#[async_trait]
impl super::PositionalReader for PositionalReader {
    type ReadAt<'a> = impl Future<Output = Result<usize>> + 'a;

    #[cfg(unix)]
    fn read_at<'a>(&'a self, buf: &'a mut [u8], offset: u64) -> Self::ReadAt<'a> {
        use std::os::unix::fs::FileExt;
        async move { self.0.read_at(buf, offset) }
    }
    async fn sync_all(&mut self) -> Result<()> {
        async move { self.0.sync_all() }.await
    }

    fn direct_io_ify(&self) -> Result<()> {
        super::direct_io_ify(self.0.as_raw_fd())
    }
}

pub struct SequentialWriter(File);

#[async_trait]
impl super::SequentialWriter for SequentialWriter {
    type Write<'a> = impl Future<Output = Result<usize>> + 'a + Send;

    fn write<'a>(&'a mut self, buf: &'a [u8]) -> Self::Write<'a> {
        use std::io::Write as _;
        async move { self.0.write(buf) }
    }

    async fn sync_data(&mut self) -> Result<()> {
        async move { self.0.sync_data() }.await
    }

    async fn sync_all(&mut self) -> Result<()> {
        async move { self.0.sync_all() }.await
    }

    async fn truncate(&self, len: u64) -> Result<()> {
        async move { self.0.set_len(len) }.await
    }

    fn direct_io_ify(&self) -> Result<()> {
        super::direct_io_ify(self.0.as_raw_fd())
    }
}

pub struct JoinHandle<T> {
    handle: Option<thread::JoinHandle<T>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let handle = self.handle.take().unwrap();
        match handle.join() {
            Ok(v) => Poll::Ready(v),
            Err(e) => std::panic::resume_unwind(e),
        }
    }
}
