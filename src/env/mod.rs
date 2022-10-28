use std::{future::Future, io::Result, path::Path};

pub use async_trait::async_trait;
use futures::future::BoxFuture;
pub use photonio::io::{Read, ReadAt, Write, WriteAt};

mod stdenv;
pub use stdenv::Std;

mod photon;
pub use photon::Photon;

///  Options to configure how the file is read.
#[derive(Default)]
pub struct ReadOptions {
    /// Pass custom flags to the `flags` argument of `open`.
    /// See [`OpenOptionsExt::custome_flags`].
    pub custome_flags: i32,
}

///  Options to configure how the file is written.
pub struct WriteOptions {
    /// Pass custom flags to the `flags` argument of `open`.
    /// See also [` os::unix::fs::OpenOptionsExt::custome_flags`].
    pub custome_flags: i32,

    /// Sets the option to create a new file, or open it if it already exists.
    /// See also [`std::fs::OpenOptions::create`].
    pub create: bool,

    /// Sets the option for truncating a previous file.
    /// See also [`std::fs::OpenOptions::truncate`].
    pub truncate: bool,

    /// Sets the option for the append mode.
    /// See also [`std::fs::OpenOptions::append`].
    pub append: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            custome_flags: 0,
            create: true,
            truncate: true,
            append: false,
        }
    }
}

/// Provides an environment to interact with a specific platform.
#[async_trait]
pub trait Env: Clone + Send + Sync {
    type PositionalReader: ReadAt + Syncer + Send + Sync + 'static;
    type SequentialWriter: Write + Syncer + Send;

    /// Opens a file for positional reads.
    async fn open_positional_reader<P>(
        &self,
        path: P,
        opt: ReadOptions,
    ) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send;

    /// Opens a file for sequential writes.
    async fn open_sequential_writer<P>(
        &self,
        path: P,
        opt: WriteOptions,
    ) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send;

    /// Spawns a task to run in the background.
    fn spawn_background<'a, F>(&self, f: F) -> BoxFuture<'a, F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send;

    /// An async version of [`std::fs::rename`].
    async fn rename<P: AsRef<Path> + Send, Q: AsRef<Path> + Send>(
        &self,
        from: P,
        to: Q,
    ) -> Result<()>;

    /// Removes a file from the filesystem.
    /// See also [`std::fs::remove_file`].
    async fn remove_file<P: AsRef<Path> + Send>(&self, path: P) -> Result<()>;

    /// Recursively create a directory and all of its parent components if they
    /// are missing.
    /// See also [`std::fs::create_dir_all`].
    async fn create_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()>;

    /// Removes a directory at this path, after removing all its contents.
    /// See also [`std::fs::remove_dir_all`].
    async fn remove_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()>;

    /// Returns an iterator over the entries within a directory.
    /// See alos [`std::fs::read_dir`].
    /// TODO: async iterator impl?
    fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<std::fs::ReadDir>;

    /// Given a path, query the file system to get information about a file,
    /// directory, etc.
    /// See alos [`std::fs::metadata`].
    async fn metadata<P: AsRef<Path> + Send>(&self, path: P) -> Result<Metadata>;
}

/// Synchronizes modified for the file.
pub trait Syncer {
    /// A future that resolves to the result of [`Self::sync_data`].
    type SyncData<'a>: Future<Output = Result<()>> + 'a + Send
    where
        Self: 'a;

    ///  Synchronizes all modified content but without metadata of this file to
    /// disk.
    ///
    /// Returns Ok when success.
    fn sync_data(&mut self) -> Self::SyncData<'_>;

    /// A future that resolves to the result of [`Self::sync_all`].
    type SyncAll<'b>: Future<Output = Result<()>> + 'b + Send
    where
        Self: 'b;
    ///  Synchronizes all modified data (include metadata) this file to disk.
    ///
    /// Returns Ok when success.
    fn sync_all(&mut self) -> Self::SyncAll<'_>;
}

/// Metadata information about a file.
#[allow(clippy::len_without_is_empty)]
pub struct Metadata {
    /// The size of the file this metadata is for.
    pub len: u64,

    /// Is this metadata for a directory.
    pub is_dir: bool,
}
