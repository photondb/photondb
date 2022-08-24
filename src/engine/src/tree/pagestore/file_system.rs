use std::{
    io::Result,
    path::{Path, PathBuf},
};

use super::manifest_file::ManifestFileWriter;
use crate::env::Env;

struct FilePath {
    root: PathBuf,
}

impl FilePath {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn root(&self) -> &Path {
        self.root.as_path()
    }

    fn lock_file(&self) -> PathBuf {
        self.root.as_path().join("LOCK")
    }

    fn current_file(&self) -> PathBuf {
        self.root.as_path().join("CURRENT")
    }

    fn manifest_file(&self, number: u64) -> PathBuf {
        let name = format!("MANIFEST-{}", number);
        self.root.as_path().join(name)
    }
}

pub struct FileSystem<E: Env> {
    env: E,
    dir: FilePath,
    manifest_writer: ManifestFileWriter<E::SequentialFile>,
}

impl<E: Env> FileSystem<E> {
    pub async fn open(env: E, root: PathBuf) -> Result<Self> {
        let dir = FilePath::new(root);
        let this = if !env.path_exists(dir.current_file()).await? {
            Self::init(env, dir).await?
        } else {
            Self::recover(env, dir).await?
        };
        Ok(this)
    }

    async fn init(env: E, root: FilePath) -> Result<Self> {
        env.make_dir(root.root()).await?;
        env.lock_file(root.lock_file()).await?;
        todo!()
    }

    async fn recover(env: E, path: FilePath) -> Result<Self> {
        env.lock_file(path.lock_file()).await?;
        todo!()
    }

    pub async fn close(&self) -> Result<()> {
        self.env.unlock_file(self.dir.lock_file()).await?;
        Ok(())
    }
}
