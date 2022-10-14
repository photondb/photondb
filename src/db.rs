use std::{io::Result, path::Path};

use crate::env::{Env, Photon};

#[derive(Default)]
#[non_exhaustive]
pub struct Options {}

pub struct Db<E> {
    env: E,
    options: Options,
}

impl<E: Env> Db<E> {
    pub async fn open<P: AsRef<Path>>(env: E, root: P, options: Options) -> Result<Self> {
        Ok(Db { env, options })
    }

    pub async fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>> {
        self.get_with(key, lsn, |value| value.map(|v| v.to_owned()))
            .await
    }

    pub async fn get_with<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        todo!()
    }

    pub async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        Ok(())
    }

    pub async fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        Ok(())
    }
}
