use std::path::Path;

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::Result,
    tree::Tree,
    Options,
};

pub(crate) struct Table<E> {
    tree: Tree<E>,
}

impl<E: Env> Table<E> {
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let tree = Tree::open(env, path, options).await?;
        Ok(Self { tree })
    }

    pub(crate) async fn get<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let key = Key::new(key, lsn);
        self.tree.get(key, f).await
    }

    pub(crate) async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        self.tree.write(key, value).await
    }

    pub(crate) async fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        self.tree.write(key, value).await
    }
}
