use ::std::{future::Future, path::Path};
pub use photonio::io::{Read, ReadAt, Result, Write, WriteAt};

pub mod std;

/// Provides an environment to interact with a specific platform.
pub trait Env {
    type PositionalReader: ReadAt;
    type OpenPositionalReader: Future<Output = Result<Self::PositionalReader>>;
    /// Opens a file for positional reads.
    fn open_positional_reader(&self, path: &Path) -> Self::OpenPositionalReader;

    type SequentialWriter: Write;
    type OpenSequentialWriter: Future<Output = Result<Self::SequentialWriter>>;
    /// Opens a file for sequential writes.
    fn open_sequential_writer(&self, path: &Path) -> Self::OpenSequentialWriter;

    type JoinHandle: Future<Output = ()>;
    /// Spawns a task to run in the background.
    fn spawn_background<F>(&self, f: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static;
}
