pub use ::photonio::io::{Read, ReadAt, Write, WriteAt};
use ::std::{future::Future, io::Result, path::Path};
pub use async_trait::async_trait;

pub mod photonio;
pub mod std;

/// Provides an environment to interact with a specific platform.
#[async_trait]
pub trait Env {
    type PositionalReader: ReadAt;
    type SequentialWriter: Write;

    /// Opens a file for positional reads.
    async fn open_positional_reader<P>(&self, path: P) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send;

    /// Opens a file for sequential writes.
    async fn open_sequential_writer<P>(&self, path: P) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send;

    /// Spawns a task to run in the background.
    async fn spawn_background<F>(&self, f: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send;
}
