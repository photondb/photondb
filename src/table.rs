use std::path::Path;

use crate::{
    env::{Env, Photon},
    page::{Key, Value},
    tree::{Stats, Tree},
    util::atomic::Sequencer,
    Options, Result,
};

pub struct Table {
    raw: RawTable<Photon>,
    lsn: Sequencer,
}

impl Table {
    /// Opens a table in the path.
    pub async fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let raw = RawTable::open(Photon, path, options).await?;
        Ok(Self {
            raw,
            lsn: Sequencer::new(0),
        })
    }

    /// Gets the value corresponding to the key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let lsn = self.lsn.get();
        self.raw
            .get(key, lsn, |value| value.map(|value| value.to_vec()))
            .await
    }

    /// Inserts the key-value pair into the table.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let lsn = self.lsn.inc();
        self.raw.put(key, lsn, value).await
    }

    /// Deletes the key from the table.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let lsn = self.lsn.inc();
        self.raw.delete(key, lsn).await
    }

    /// Returns the statistics of the table.
    pub fn stats(&self) -> Stats {
        self.raw.stats()
    }
}

pub struct RawTable<E> {
    tree: Tree<E>,
}

impl<E: Env> RawTable<E> {
    pub async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let tree = Tree::open(env, path, options).await?;
        Ok(Self { tree })
    }

    pub async fn get<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let key = Key::new(key, lsn);
        let result = self.tree.get(key, f).await?;
        Ok(result)
    }

    pub async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        self.tree.write(key, value).await?;
        Ok(())
    }

    pub async fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        self.tree.write(key, value).await?;
        Ok(())
    }

    pub fn stats(&self) -> Stats {
        self.tree.stats()
    }
}
