use std::path::Path;

mod page;

mod stats;
use stats::AtomicStats;
pub(crate) use stats::Stats;

mod tree_txn;
use tree_txn::TreeTxn;

mod rewrite;
pub(crate) use rewrite::PageRewriter;

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::{Error, PageStore, Result},
    table::Options,
};

/// A latch-free, log-structured tree.
pub(crate) struct Tree<E: Env> {
    options: Options,
    stats: AtomicStats,
    store: PageStore<E>,
}

impl<E: Env> Tree<E> {
    /// Opens a tree in the path.
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let stats = AtomicStats::default();
        let store = PageStore::open(env, path, options.page_store.clone()).await?;
        Ok(Self {
            options,
            stats,
            store,
        })
    }

    fn begin(&self) -> TreeTxn<E> {
        TreeTxn::new(self)
    }

    /// Gets the value corresponding to the key and applies the function to it.
    pub(crate) async fn get<F, R>(&self, key: Key<'_>, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        loop {
            let txn = self.begin();
            match txn.get(key).await {
                Ok(value) => {
                    self.stats.success.get.inc();
                    return Ok(f(value));
                }
                Err(Error::Again) => {
                    self.stats.restart.get.inc();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Writes the key-value pair to the tree.
    pub(crate) async fn write(&self, key: Key<'_>, value: Value<'_>) -> Result<()> {
        loop {
            let txn = self.begin();
            match txn.write(key, value).await {
                Ok(_) => {
                    self.stats.success.write.inc();
                    return Ok(());
                }
                Err(Error::Again) => {
                    self.stats.restart.write.inc();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Rewrites the corresponding page.
    pub(crate) async fn rewrite(&self, page_id: u64) -> Result<()> {
        loop {
            let txn = self.begin();
            match txn.rewrite(page_id).await {
                Ok(_) => return Ok(()),
                Err(Error::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Returns the statistics of the tree.
    pub(crate) fn stats(&self) -> Stats {
        self.stats.snapshot()
    }

    pub(crate) fn store(&self) -> &PageStore<E> {
        &self.store
    }
}
