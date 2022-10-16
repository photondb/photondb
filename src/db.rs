use std::path::Path;

use crate::{
    data::{Entry, Key},
    env::Env,
    tree::Tree,
    Result,
};

#[non_exhaustive]
pub struct Options {
    pub page_chain_length: usize,
}

impl Options {
    pub fn new() -> Self {
        Self {
            page_chain_length: 8,
        }
    }
}

pub struct Db<E> {
    env: E,
    tree: Tree,
}

impl<E: Env> Db<E> {
    pub async fn open<P: AsRef<Path>>(env: E, root: P, options: Options) -> Result<Self> {
        let tree = Tree::open(options).await?;
        Ok(Db { env, tree })
    }

    pub async fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>> {
        self.get_with(key, lsn, |value| value.map(|v| v.to_owned()))
            .await
    }

    pub async fn get_with<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let key = Key::new(key, lsn);
        self.tree.get(key, f).await
    }

    pub async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let key = Key::new(key, lsn);
        let entry = Entry::put(key, value);
        self.tree.write(entry).await
    }

    pub async fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let key = Key::new(key, lsn);
        let entry = Entry::delete(key);
        self.tree.write(entry).await
    }
}
