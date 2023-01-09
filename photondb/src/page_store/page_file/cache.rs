use std::{marker::PhantomData, sync::Arc};

use futures::Future;

use super::FileReader;
use crate::{
    env::Env,
    page_store::{cache::Cache, stats::CacheStats, CacheOption, Error, LRUCache, Result},
};

pub(super) struct FileReaderCache<E: Env> {
    cache: Arc<LRUCache<Arc<FileReader<E::PositionalReader>>>>,
    _marker: PhantomData<E>,
}

impl<E: Env> FileReaderCache<E> {
    pub(super) fn new(max_size: u64) -> Self {
        let cache = Arc::new(LRUCache::new(max_size as usize, -1, 0.0, 0.0));
        Self {
            cache,
            _marker: PhantomData,
        }
    }

    pub(super) async fn get_with(
        &self,
        file_id: u32,
        init: impl Future<Output = Result<Arc<FileReader<E::PositionalReader>>>>,
    ) -> Result<Arc<FileReader<E::PositionalReader>>> {
        let key = file_id as u64;
        if let Some(cached) = self.cache.lookup(key) {
            return Ok(cached.value().clone());
        }
        let reader = init.await?;
        match self
            .cache
            .insert(key, Some(reader.clone()), 1, CacheOption::default())
        {
            Ok(_) | Err(Error::MemoryLimit) => {}
            Err(err) => return Err(err),
        }
        Ok(reader)
    }

    pub(super) fn invalidate(&self, file_id: u32) {
        self.cache.erase(file_id as u64);
    }

    pub(super) fn stats(&self) -> CacheStats {
        self.cache.stats()
    }
}
