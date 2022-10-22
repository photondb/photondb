use std::path::Path;

mod txn;
use txn::Txn;

mod iter;
mod stats;
use stats::AtomicStats;
pub(crate) use stats::Stats;

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::{Error, PageStore, Result},
    Options,
};

/// A latch-free, log-structured tree.
pub(crate) struct Tree<E> {
    options: Options,
    stats: AtomicStats,
    store: PageStore<E>,
}

impl<E: Env> Tree<E> {
    /// Opens a tree in the path.
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let stats = AtomicStats::default();
        let store = PageStore::open(env, path, options.clone()).await?;
        Ok(Self {
            options,
            stats,
            store,
        })
    }

    fn begin(&self) -> Txn<E> {
        Txn::new(self)
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

    /// Returns the statistics of the tree.
    pub(crate) fn stats(&self) -> Stats {
        self.stats.snapshot()
    }
}
