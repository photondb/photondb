use std::path::Path;

use crate::{Env, Result};

#[non_exhaustive]
pub struct Options<E> {
    env: E,
}

pub struct Db<E> {
    options: Options<E>,
}

impl<E: Env> Db<E> {
    pub async fn open<P: AsRef<Path>>(root: P, options: Options<E>) -> Result<Self> {
        Ok(Db { options })
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
