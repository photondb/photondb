use std::{path::Path, sync::Arc};

use crate::{
    env::Env,
    page::{Key, Value},
    page_store::{JobHandle, MinDeclineRateStrategyBuilder, Options as PageStoreOptions},
    tree::{PageRewriter, Stats, Tree},
    Result,
};

/// Options to configure a table.
#[non_exhaustive]
#[derive(Clone)]
pub struct Options {
    /// Approximate size of user data packed per page before it is split.
    ///
    /// Note that the size specified here corresponds to uncompressed data.
    ///
    /// Default: 8KB
    pub page_size: usize,

    /// Approximate number of delta pages chained per page before it is
    /// consolidated.
    ///
    /// Default: 4
    pub page_chain_length: usize,

    pub page_store: PageStoreOptions,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            page_size: 8 << 10,
            page_chain_length: 4,
            page_store: PageStoreOptions::default(),
        }
    }
}

pub struct Table<E: Env + 'static> {
    tree: Arc<Tree<E>>,
    _job_guard: JobHandle,
}

impl<E: Env + 'static> Table<E> {
    pub async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let tree = Arc::new(Tree::open(env, path, options).await?);
        let rewriter = Box::new(PageRewriter::new(tree.clone()));
        // TODO: add options.
        let strategy_builder = Box::new(MinDeclineRateStrategyBuilder::new(1 << 30, usize::MAX));
        let _job_guard = JobHandle::new(tree.store(), rewriter, strategy_builder);
        Ok(Self { tree, _job_guard })
    }

    pub async fn get<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let key = Key::new(key, lsn);
        let result = self.tree.get(key, f).await?;
        Ok(result)
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

    pub fn stats(&self) -> Stats {
        self.tree.stats()
    }
}
