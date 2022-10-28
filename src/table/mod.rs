use std::{path::Path, sync::Arc};

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::{Error, JobHandle, MinDeclineRateStrategyBuilder, PageStore},
    Result,
};

mod options;
pub use options::{Options, ReadOptions, WriteOptions};

mod page;

mod stats;
use stats::AtomicStats;
pub(crate) use stats::Stats;

mod tree;
use tree::Tree;

pub struct Table<E: Env> {
    tree: Arc<Tree<E>>,
    store: PageStore<E>,
    _job_guard: JobHandle,
}

impl<E: Env + 'static> Table<E> {
    /// Opens a tree in the path.
    pub async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let tree = Arc::new(Tree::new(options.clone()));
        let store = PageStore::open(env, path, options.page_store).await?;
        let rewriter = Box::new(tree.clone());
        // TODO: add options.
        let strategy_builder = Box::new(MinDeclineRateStrategyBuilder::new(1 << 30, usize::MAX));
        let _job_guard = JobHandle::new(&store, rewriter, strategy_builder);
        Ok(Self {
            tree,
            store,
            _job_guard,
        })
    }

    /// Gets the value corresponding to the key and applies the function to it.
    pub async fn get<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let key = Key::new(key, lsn);
        loop {
            let guard = self.store.guard();
            let session = self.tree.begin(&guard);
            match session.get(key).await {
                Ok(value) => {
                    self.tree.stats.success.get.inc();
                    return Ok(f(value));
                }
                Err(Error::Again) => {
                    self.tree.stats.restart.get.inc();
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
            let session = self.tree.begin(&guard);
            match session.write(key, value).await {
                Ok(_) => {
                    self.tree.stats.success.write.inc();
                    return Ok(());
                }
                Err(Error::Again) => {
                    self.tree.stats.restart.write.inc();
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Returns the statistics of the table.
    pub fn stats(&self) -> Stats {
        self.tree.stats.snapshot()
    }
}
