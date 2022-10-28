use std::{
    future::Future,
    io::Result,
    os::fd::AsRawFd,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;
use photonio::{
    fs::{File, OpenOptions},
    task,
};

use super::*;

/// An implementation of [`Env`] based on PhotonIO.
#[derive(Clone)]
pub struct Photon;

#[async_trait]
impl Env for Photon {
    type PositionalReader = PositionalReader;
    type SequentialWriter = SequentialWriter;
    type JoinHandle<T: Send> = JoinHandle<T>;

    async fn open_positional_reader<P>(&self, path: P) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        let r = OpenOptions::new().read(true).open(path).await?;
        Ok(PositionalReader(r))
    }

    async fn open_sequential_writer<P>(
        &self,
        path: P,
        opt: WriteOptions,
    ) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send,
    {
        let w = if opt.append {
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(path)
                .await
        } else {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .await
        }?;
        Ok(SequentialWriter(w))
    }

    fn spawn_background<F>(&self, f: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let handle = task::spawn(f);
        JoinHandle { handle }
    }

    /// An async version of [`std::fs::rename`].
    async fn rename<P: AsRef<Path> + Send, Q: AsRef<Path> + Send>(
        &self,
        from: P,
        to: Q,
    ) -> Result<()> {
        photonio::fs::rename(from, to).await
    }

    /// An async version of [`std::fs::remove_file`].
    async fn remove_file<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        photonio::fs::remove_file(path).await
    }

    /// An async version of [`std::fs::create_dir`].
    async fn create_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        std::fs::create_dir_all(path) // TODO: async impl
    }

    /// An async version of [`std::fs::remove_dir`].
    async fn remove_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        std::fs::remove_dir_all(path) // TODO: async impl
    }

    /// Returns an iterator over the entries within a directory.
    /// See alos [`std::fs::read_dir`].
    fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<std::fs::ReadDir> {
        std::fs::read_dir(path)
    }

    async fn metadata<P: AsRef<Path> + Send>(&self, path: P) -> Result<Metadata> {
        let path = path.as_ref();
        let file = File::open(path).await?;
        let raw_metadata = file.metadata().await?;
        let metadata = Metadata {
            len: raw_metadata.len(),
            is_dir: raw_metadata.is_dir(),
        };
        Ok(metadata)
    }
}

pub struct SequentialWriter(File);

#[async_trait]
impl super::SequentialWriter for SequentialWriter {
    type Write<'a> = impl Future<Output = Result<usize>> + 'a + Send;

    fn write<'a>(&'a mut self, buf: &'a [u8]) -> Self::Write<'a> {
        self.0.write(buf)
    }

    async fn sync_data(&mut self) -> Result<()> {
        self.0.sync_data().await
    }

    async fn sync_all(&mut self) -> Result<()> {
        self.0.sync_all().await
    }

    async fn truncate(&self, len: u64) -> Result<()> {
        self.0.set_len(len).await
    }

    fn direct_io_ify(&self) -> Result<()> {
        super::direct_io_ify(self.0.as_raw_fd())
    }
}

pub struct PositionalReader(File);

#[async_trait]
impl super::PositionalReader for PositionalReader {
    type ReadAt<'a> = impl Future<Output = Result<usize>> + 'a;

    fn read_at<'a>(&'a self, buf: &'a mut [u8], pos: u64) -> Self::ReadAt<'a> {
        self.0.read_at(buf, pos)
    }

    async fn sync_all(&mut self) -> Result<()> {
        self.0.sync_all().await
    }

    fn direct_io_ify(&self) -> Result<()> {
        super::direct_io_ify(self.0.as_raw_fd())
    }
}

pub struct JoinHandle<T> {
    handle: task::JoinHandle<T>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.handle.poll_unpin(cx) {
            Poll::Ready(Ok(v)) => Poll::Ready(v),
            Poll::Ready(Err(e)) => panic!("JoinHandle error: {:?}", e),
            Poll::Pending => Poll::Pending,
        }
    }
}
