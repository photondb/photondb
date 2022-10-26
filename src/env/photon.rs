use std::{future::Future, io::Result, path::Path};

use photonio::{fs::File, task};

use super::{async_trait, Env};

/// An implementation of [`Env`] based on PhotonIO.
pub struct Photon;

#[async_trait]
impl Env for Photon {
    type PositionalReader = File;
    type SequentialWriter = File;

    async fn open_positional_reader<P>(&self, path: P) -> Result<Self::PositionalReader>
    where
        P: AsRef<Path> + Send + Sync,
    {
        File::open(path).await
    }

    async fn open_sequential_writer<P>(&self, path: P) -> Result<Self::SequentialWriter>
    where
        P: AsRef<Path> + Send + Sync,
    {
        File::create(path).await
    }

    async fn spawn_background<F>(&self, f: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        task::spawn(f).await.unwrap()
    }
}
