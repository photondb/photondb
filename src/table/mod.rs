use std::{path::Path, sync::Arc};

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::{Error, PageStore},
    Result,
};

mod options;
pub use options::{Options, ReadOptions, WriteOptions};

mod stats;
pub use stats::Stats;

mod page;
mod tree;
use tree::Tree;

pub struct Table<E: Env> {
    tree: Arc<Tree>,
    store: PageStore<E>,
}

impl<E: Env> Table<E> {
    /// Opens a table in the path with the given options.
    pub async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let tree = Arc::new(Tree::new(options.clone()));
        let rewriter = Box::new(tree.clone());
        let store = PageStore::open(env, path, options.page_store, rewriter).await?;
        let guard = store.guard();
        let txn = tree.begin(&guard);
        txn.init().await?;
        Ok(Self { tree, store })
    }

    /// Gets the value corresponding to the key and applies the function to it.
    pub async fn get<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let key = Key::new(key, lsn);
        loop {
            let guard = self.store.guard();
            let txn = self.tree.begin(&guard);
            match txn.get(key).await {
                Ok(value) => {
                    return Ok(f(value));
                }
                Err(Error::Again) => {
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        self.write(key, value).await?;
        Ok(())
    }

    pub async fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        self.write(key, value).await?;
        Ok(())
    }

    async fn write(&self, key: Key<'_>, value: Value<'_>) -> Result<()> {
        loop {
            let guard = self.store.guard();
            let txn = self.tree.begin(&guard);
            match txn.write(key, value).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(Error::Again) => {
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Returns the statistics of the table.
    pub fn stats(&self) -> Stats {
        self.tree.stats()
    }

    /// Returns the minimal LSN that the table allows to read with.
    pub fn safe_lsn(&self) -> u64 {
        self.tree.safe_lsn()
    }

    /// Updates the minimal LSN that the table allows to read with.
    pub fn set_safe_lsn(&self, lsn: u64) {
        self.tree.set_safe_lsn(lsn);
    }
}
