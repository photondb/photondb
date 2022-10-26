use std::{future::Future, io::Result, path::Path};

pub use async_trait::async_trait;
use futures::future::BoxFuture;
pub use photonio::io::{Read, ReadAt, Write, WriteAt};

mod sync;
pub use sync::Sync;

mod photon;
pub use photon::Photon;

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
    fn spawn_background<F>(&self, f: F) -> BoxFuture<'static, F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send;
}
