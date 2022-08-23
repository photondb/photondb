use std::{io::Result, path::PathBuf};

use crate::env::Env;

pub struct FilePath {
    root: PathBuf,
}

impl FilePath {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn lock_file(&self) -> PathBuf {
        self.root.as_path().join("LOCK")
    }

    pub fn current_file(&self) -> PathBuf {
        self.root.as_path().join("CURRENT")
    }

    pub fn manifest_file(&self, number: u64) -> PathBuf {
        let name = format!("MANIFEST-{}", number);
        self.root.as_path().join(name)
    }
}

pub struct FileSystem<E> {
    env: E,
    root: FilePath,
}

impl<E: Env> FileSystem<E> {
    pub async fn open(env: E, root: PathBuf) -> Result<Self> {
        let root = FilePath::new(root);
        Ok(Self { env, root })
    }

    pub async fn lock(&self) -> Result<()> {
        todo!()
    }

    pub async fn unlock(self) -> Result<()> {
        todo!()
    }
}
