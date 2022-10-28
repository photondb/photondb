use std::{path::Path, sync::Arc};

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::{Error, JobHandle, MinDeclineRateStrategyBuilder, PageStore, RewritePage},
    Result,
};

mod options;
pub use options::{Options, ReadOptions, WriteOptions};

mod page;

mod stats;
use stats::AtomicStats;
pub(crate) use stats::Stats;

mod tree_txn;
use tree_txn::TreeTxn;

pub struct Table<E: Env> {
    tree: Arc<Tree<E>>,
    _job_guard: JobHandle,
}

impl<E: Env + 'static> Table<E> {
    /// Opens a tree in the path.
    pub async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let stats = AtomicStats::default();
        let store = PageStore::open(env, path, options.page_store.clone()).await?;
        let tree = Arc::new(Tree {
            options,
            stats,
            store,
        });
        let rewriter = Box::new(tree.clone());
        // TODO: add options.
        let strategy_builder = Box::new(MinDeclineRateStrategyBuilder::new(1 << 30, usize::MAX));
        let _job_guard = JobHandle::new(&tree.store, rewriter, strategy_builder);
        Ok(Self { tree, _job_guard })
    }

    /// Gets the value corresponding to the key and applies the function to it.
    pub async fn get<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let key = Key::new(key, lsn);
        self.tree.get(key, f).await
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

    /// Returns the statistics of the tree.
    pub fn stats(&self) -> Stats {
        self.tree.stats.snapshot()
    }
}

struct Tree<E: Env> {
    options: Options,
    stats: AtomicStats,
    store: PageStore<E>,
}

impl<E: Env + 'static> Tree<E> {
    fn begin(&self) -> TreeTxn<E> {
        TreeTxn::new(self)
    }

    async fn get<F, R>(&self, key: Key<'_>, f: F) -> Result<R>
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
                Err(e) => return Err(e.into()),
            }
        }
    }

    async fn write(&self, key: Key<'_>, value: Value<'_>) -> Result<()> {
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
                Err(e) => return Err(e.into()),
            }
        }
    }
}

#[async_trait::async_trait]
impl<E: Env + 'static> RewritePage for Arc<Tree<E>> {
    async fn rewrite(&self, page_id: u64) -> crate::page_store::Result<()> {
        loop {
            let txn = self.begin();
            match txn.rewrite_page(page_id).await {
                Ok(_) => return Ok(()),
                Err(Error::Again) => continue,
                Err(e) => return Err(e),
            }
        }
    }
}
