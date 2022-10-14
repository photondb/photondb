use std::{future::Future, io::Result, path::Path};

use photonio::{fs::File, runtime::Runtime};

/// An implementation of [`super::Env`] based on [`photonio`][photonio].
///
/// [photonio]: https://github.com/photondb/photonio.
pub struct Env {
    rt: Runtime,
}

#[super::async_trait]
impl super::Env for Env {
    type PositionalReader = File;
    type SequentialWriter = File;

    async fn open_positional_reader<P>(&self, path: P) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send,
    {
        File::open(path).await
    }

    async fn open_sequential_writer<P>(&self, path: P) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send,
    {
        File::create(path).await
    }

    async fn spawn_background<F>(&self, f: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let handle = self.rt.spawn(f);
        handle.await.unwrap()
    }
}
