use std::{
    future::Future,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::{Error, Guard, PageStore, RewritePage},
    Result,
};

mod options;
pub use options::{Options, ReadOptions, WriteOptions};

mod stats;
use stats::AtomicStats;
pub use stats::Stats;

mod page;
mod tree;
use tree::TreeTxn;

pub struct Table<E: Env> {
    tree: Arc<Tree>,
    store: PageStore<E>,
}

impl<E: Env> Table<E> {
    /// Opens a table in the path with the given options.
    pub async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let tree = Arc::new(Tree::new(options.clone()));
        let store = PageStore::open(env, path, options.page_store, tree.clone()).await?;
        let txn = tree.begin(store.guard());
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
            let txn = self.tree.begin(self.store.guard());
            match txn.get(key).await {
                Ok(value) => {
                    self.tree.stats.success.get.inc();
                    return Ok(f(value));
                }
                Err(Error::Again) => {
                    self.tree.stats.failure.get.inc();
                    continue;
                }
                Err(e) => {
                    self.tree.stats.failure.get.inc();
                    return Err(e.into());
                }
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
            let txn = self.tree.begin(self.store.guard());
            match txn.write(key, value).await {
                Ok(_) => {
                    self.tree.stats.success.write.inc();
                    return Ok(());
                }
                Err(Error::Again) => {
                    self.tree.stats.failure.write.inc();
                    continue;
                }
                Err(e) => {
                    self.tree.stats.failure.write.inc();
                    return Err(e.into());
                }
            }
        }
    }

    /// Returns the statistics of the table.
    pub fn stats(&self) -> Stats {
        self.tree.stats.snapshot()
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

struct Tree {
    options: Options,
    stats: AtomicStats,
    safe_lsn: AtomicU64,
}

impl Tree {
    fn new(options: Options) -> Self {
        Self {
            options,
            stats: AtomicStats::default(),
            safe_lsn: AtomicU64::new(0),
        }
    }

    fn begin<'a, E: Env>(&'a self, guard: Guard<'a, E>) -> TreeTxn<'a, E> {
        TreeTxn::new(self, guard)
    }

    fn safe_lsn(&self) -> u64 {
        self.safe_lsn.load(Ordering::Acquire)
    }

    fn set_safe_lsn(&self, lsn: u64) {
        loop {
            let safe_lsn = self.safe_lsn.load(Ordering::Acquire);
            // Make sure that the safe LSN is increasing.
            if safe_lsn >= lsn {
                return;
            }
            if self
                .safe_lsn
                .compare_exchange(safe_lsn, lsn, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
}

impl<E: Env> RewritePage<E> for Arc<Tree> {
    type Rewrite<'a> = impl Future<Output = Result<(), Error>> + Send + 'a
        where
            Self: 'a;

    fn rewrite<'a>(&'a self, id: u64, guard: Guard<'a, E>) -> Self::Rewrite<'a> {
        async move {
            let txn = self.begin(guard);
            txn.rewrite_page(id).await
        }
    }
}
