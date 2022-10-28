use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::Result,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    thread,
};

use futures::{executor::block_on, future::BoxFuture};

use super::{async_trait, Env, Metadata, ReadAt, ReadOptions, Syncer, Write, WriteOptions};

/// An implementation of [`Env`] based on [`std`] with synchronous I/O.
#[derive(Clone)]
pub struct Std;

#[async_trait]
impl Env for Std {
    type PositionalReader = PositionalReader;
    type SequentialWriter = SequentialWriter;

    async fn open_positional_reader<P>(
        &self,
        path: P,
        opt: ReadOptions,
    ) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send,
    {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(opt.custome_flags)
            .open(path.as_ref())?;
        Ok(PositionalReader(file))
    }

    async fn open_sequential_writer<P>(
        &self,
        path: P,
        opt: WriteOptions,
    ) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send,
    {
        let file = OpenOptions::new()
            .write(true)
            .custom_flags(opt.custome_flags)
            .create(opt.create)
            .truncate(opt.truncate)
            .append(opt.append)
            .open(path.as_ref())?;
        Ok(SequentialWriter(file))
    }

    fn spawn_background<'a, F>(&self, f: F) -> BoxFuture<'a, F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let handle = thread::spawn(move || block_on(f));
        Box::pin(async { handle.join().unwrap() })
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

impl ReadAt for PositionalReader {
    type ReadAt<'a> = impl Future<Output = Result<usize>> + 'a;

    #[cfg(unix)]
    fn read_at<'a>(&'a self, buf: &'a mut [u8], offset: u64) -> Self::ReadAt<'a> {
        use std::os::unix::fs::FileExt;
        async move { self.0.read_at(buf, offset) }
    }
}

impl Syncer for PositionalReader {
    type SyncData<'a> = impl Future<Output = Result<()>> + 'a;

    fn sync_data(&mut self) -> Self::SyncData<'_> {
        async move { self.0.sync_data() }
    }

    type SyncAll<'b> = impl Future<Output = Result<()>> + 'b;

    fn sync_all(&mut self) -> Self::SyncAll<'_> {
        async move { self.0.sync_all() }
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

impl Syncer for SequentialWriter {
    type SyncData<'a> = impl Future<Output = Result<()>> + 'a;

    fn sync_data(&mut self) -> Self::SyncData<'_> {
        async move { self.0.sync_data() }
    }

    type SyncAll<'b> = impl Future<Output = Result<()>> + 'b;

    fn sync_all(&mut self) -> Self::SyncAll<'_> {
        async move { self.0.sync_all() }
    }
}
