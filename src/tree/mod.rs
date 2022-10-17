use std::path::Path;

mod txn;
use txn::TreeTxn;

mod stats;
use stats::AtomicTreeStats;
pub use stats::TreeStats;

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::{Error, PageStore, Result},
    Options,
};

pub(crate) struct Tree<E> {
    options: Options,
    stats: AtomicTreeStats,
    store: PageStore<E>,
}

impl<E: Env> Tree<E> {
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let stats = AtomicTreeStats::default();
        let store = PageStore::open(env, path).await?;
        Ok(Self {
            options,
            stats,
            store,
        })
    }

    fn begin(&self) -> TreeTxn {
        let guard = self.store.guard();
        TreeTxn::new(&self.options, &self.stats, guard)
    }

    pub(crate) async fn get<F, R>(&self, key: Key<'_>, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        loop {
            let txn = self.begin();
            match txn.get(&key).await {
                Ok(value) => return Ok(f(value)),
                Err(Error::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) async fn write(&self, key: Key<'_>, value: Value<'_>) -> Result<()> {
        loop {
            let txn = self.begin();
            match txn.write(&key, &value).await {
                Ok(_) => return Ok(()),
                Err(Error::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) fn stats(&self) -> TreeStats {
        self.stats.snapshot()
    }
}
