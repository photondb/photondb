use std::{future::Future, io::Result, os::fd::AsRawFd, path::Path};

use futures::future::BoxFuture;
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
    type PositionalReader = File;
    type SequentialWriter = File;

    async fn open_positional_reader<P>(&self, path: P) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        OpenOptions::new().read(true).open(path).await
    }

    async fn open_sequential_writer<P>(
        &self,
        path: P,
        opt: WriteOptions,
    ) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send,
    {
        if opt.append {
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
        }
    }

    fn spawn_background<'a, F>(&self, f: F) -> BoxFuture<'a, F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let handle = task::spawn(f);
        Box::pin(async { handle.await.unwrap() })
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

#[async_trait]
impl SequentialWriter for File {
    async fn sync_data(&mut self) -> Result<()> {
        File::sync_data(self).await
    }

    async fn sync_all(&mut self) -> Result<()> {
        File::sync_all(self).await
    }

    async fn truncate(&self, len: u64) -> Result<()> {
        File::set_len(self, len).await
    }

    fn direct_io_ify(&self) -> Result<()> {
        super::direct_io_ify(self.as_raw_fd())
    }
}

#[async_trait]
impl PositionalReader for File {
    async fn sync_all(&mut self) -> Result<()> {
        File::sync_all(self).await
    }

    fn direct_io_ify(&self) -> Result<()> {
        super::direct_io_ify(self.as_raw_fd())
    }
}
