use std::path::Path;

use crate::{
    data::{Entry, Key},
    env::Env,
    page_store::Result,
    tree::Tree,
    Options,
};

pub struct Table<E> {
    tree: Tree<E>,
}

impl<E: Env> Table<E> {
    pub(crate) async fn open<P: AsRef<Path>>(env: E, root: P, opts: Options) -> Result<Self> {
        let tree = Tree::open(env, opts).await?;
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
        let entry = Entry::put(key, value);
        self.tree.write(entry).await
    }

    pub(crate) async fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let key = Key::new(key, lsn);
        let entry = Entry::delete(key);
        self.tree.write(entry).await
    }
}
