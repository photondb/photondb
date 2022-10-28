use std::{
    future::Future,
    io::Result,
    os::unix::prelude::OpenOptionsExt,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;
use photonio::{
    fs::{File, Metadata, OpenOptions},
    task,
};

use super::{async_trait, Env, ReadOptions, Syncer, WriteOptions};

/// An implementation of [`Env`] based on PhotonIO.
#[derive(Clone)]
pub struct Photon;

#[async_trait]
impl Env for Photon {
    type PositionalReader = File;
    type SequentialWriter = File;
    type MetedataReader = Metadata;
    type JoinHandle<T: Send> = JoinHandle<T>;

    async fn open_positional_reader<P>(
        &self,
        path: P,
        opt: ReadOptions,
    ) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        OpenOptions::new()
            .read(true)
            .custom_flags(opt.custome_flags)
            .open(path)
            .await
    }

    async fn open_sequential_writer<P>(
        &self,
        path: P,
        opt: WriteOptions,
    ) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send,
    {
        OpenOptions::new()
            .write(true)
            .custom_flags(opt.custome_flags)
            .create(opt.create)
            .truncate(opt.truncate)
            .append(opt.append)
            .open(path)
            .await
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

    async fn metadata<P: AsRef<Path> + Send>(&self, path: P) -> Result<Self::MetedataReader> {
        let path = path.as_ref();
        let file = File::open(path).await?;
        let metadata = file.metadata().await?;
        Ok(metadata)
    }
}

impl Syncer for File {
    type SyncData<'a> = impl Future<Output = Result<()>> + 'a;

    fn sync_data(&mut self) -> Self::SyncData<'_> {
        File::sync_data(self)
    }

    type SyncAll<'b> = impl Future<Output = Result<()>> + 'b;

    fn sync_all(&mut self) -> Self::SyncAll<'_> {
        File::sync_all(self)
    }
}

impl super::Metadata for Metadata {
    fn len(&self) -> u64 {
        Metadata::len(self)
    }

    fn is_dir(&self) -> bool {
        Metadata::is_dir(self)
    }

    fn is_file(&self) -> bool {
        Metadata::is_file(self)
    }

    fn is_symlink(&self) -> bool {
        Metadata::is_symlink(self)
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
