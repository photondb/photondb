//! Environments for PhotonDB to interact with different runtimes and platforms.

use std::{future::Future, io::Result, path::Path};

pub use async_trait::async_trait;
pub use photonio::io::{Read, ReadAt, Write, WriteAt};

mod stdenv;
pub use stdenv::Std;

mod photon;
pub use photon::Photon;

/// Provides an environment to interact with a specific platform.
#[async_trait]
pub trait Env: Clone + Send + Sync + 'static {
    /// Positional readers returned by the environment.
    type PositionalReader: PositionalReader;
    /// Sequential writers returned by the environment.
    type SequentialWriter: SequentialWriter;
    /// Handles to await tasks spawned by the environment.
    type JoinHandle<T: Send>: Future<Output = T> + Send;
    /// Directories returned by the environment.
    type Directory: Directory + Send + Sync + 'static;

    /// Opens a file for positional reads.
    async fn open_positional_reader<P>(&self, path: P) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send;

    /// Opens a file for sequential writes.
    async fn open_sequential_writer<P>(&self, path: P) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send;

    /// Spawns a task to run in the background.
    fn spawn_background<F>(&self, f: F) -> Self::JoinHandle<F::Output>
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
    /// See also [`std::fs::read_dir`].
    /// TODO: async iterator impl?
    fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<std::fs::ReadDir>;

    /// Given a path, query the file system to get information about a file,
    /// directory, etc.
    /// See also [`std::fs::metadata`].
    async fn metadata<P: AsRef<Path> + Send>(&self, path: P) -> Result<Metadata>;

    /// Open the directory.
    async fn open_dir<P: AsRef<Path> + Send>(&self, path: P) -> Result<Self::Directory>;
}

/// A reader that allows positional reads.
#[async_trait]
pub trait PositionalReader: Send + Sync + 'static {
    /// A future that resolves to the result of [`Self::read_at`].
    type ReadAt<'a>: Future<Output = Result<usize>> + 'a + Send
    where
        Self: 'a;

    /// Reads some bytes from this object at `pos` into `buf`.
    ///
    /// Returns the number of bytes read.
    fn read_at<'a>(&'a self, buf: &'a mut [u8], pos: u64) -> Self::ReadAt<'a>;

    /// Enable direct_io for the reader.
    /// return error if direct_io unsupported.
    fn direct_io_ify(&self) -> Result<()>;
}

/// Extension methods for [`PositionalReader`].
pub trait PositionalReaderExt {
    /// A future that resolves to the result of [`Self::read_exact_at`].
    type ReadExactAt<'a>: Future<Output = Result<()>> + 'a
    where
        Self: 'a;

    /// Reads the exact number of bytes from this object at `pos` to fill `buf`.
    fn read_exact_at<'a>(&'a self, buf: &'a mut [u8], pos: u64) -> Self::ReadExactAt<'a>;
}

impl<T> PositionalReaderExt for T
where
    T: PositionalReader,
{
    type ReadExactAt<'a> = impl Future<Output = Result<()>> + 'a where Self: 'a;

    fn read_exact_at<'a>(&'a self, mut buf: &'a mut [u8], mut pos: u64) -> Self::ReadExactAt<'a> {
        async move {
            while !buf.is_empty() {
                match self.read_at(buf, pos).await {
                    Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                    Ok(n) => {
                        buf = &mut buf[n..];
                        pos += n as u64;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        }
    }
}

/// A writer that allows sequential writes.
#[async_trait]
pub trait SequentialWriter: Send + Sync + 'static {
    /// A future that resolves to the result of [`Self::write`].
    type Write<'a>: Future<Output = Result<usize>> + 'a + Send
    where
        Self: 'a;

    /// Writes some bytes from `buf` into this object.
    ///
    /// Returns the number of bytes written.
    fn write<'a>(&'a mut self, buf: &'a [u8]) -> Self::Write<'a>;

    ///  Synchronizes all modified content but without metadata of this file to
    /// disk.
    ///
    /// Returns Ok when success.
    async fn sync_data(&mut self) -> Result<()>;

    /// Returns Ok when success.
    async fn sync_all(&mut self) -> Result<()>;

    /// Truncate the writtern file to a specified length.
    async fn truncate(&self, len: u64) -> Result<()>;

    /// Enable direct_io for the writer.
    /// return error if direct_io unsupported.
    fn direct_io_ify(&self) -> Result<()>;
}

/// Provides extension methods for [`SequentialWriter`].
pub trait SequentialWriterExt {
    /// A future that resolves to the result of [`Self::write_all`].
    type WriteAll<'a>: Future<Output = Result<()>> + 'a
    where
        Self: 'a;

    /// Writes all bytes from `buf` into this object.
    fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> Self::WriteAll<'a>;
}

/// Extension methods for [`SequentialWriter`].
impl<T> SequentialWriterExt for T
where
    T: SequentialWriter,
{
    type WriteAll<'a> = impl Future<Output = Result<()>> + 'a
    where
        Self: 'a;

    fn write_all<'a>(&'a mut self, mut buf: &'a [u8]) -> Self::WriteAll<'a> {
        async move {
            while !buf.is_empty() {
                match self.write(buf).await {
                    Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
                    Ok(n) => buf = &buf[n..],
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        }
    }
}

/// Metadata information about a file.
#[allow(clippy::len_without_is_empty)]
pub struct Metadata {
    /// The size of the file this metadata is for.
    pub len: u64,

    /// Is this metadata for a directory.
    pub is_dir: bool,
}

#[cfg(target_os = "linux")]
pub(in crate::env) fn direct_io_ify(fd: i32) -> Result<()> {
    macro_rules! syscall {
            ($fn: ident ( $($arg: expr),* $(,)* ) ) => {{
                #[allow(unused_unsafe)]
                let res = unsafe { libc::$fn($($arg, )*) };
                if res == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(res)
                }
            }};
        }
    let flags = syscall!(fcntl(fd, libc::F_GETFL))?;
    syscall!(fcntl(fd, libc::F_SETFL, flags | libc::O_DIRECT))?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(in crate::env) fn direct_io_ify(_: i32) -> Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "enable direct io fail",
    ))
}

/// A handle to an opened directory.
#[async_trait]
pub trait Directory {
    /// Sync_all directory.
    async fn sync_all(&self) -> Result<()>;
}
