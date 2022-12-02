use std::sync::Arc;

use async_trait::async_trait;
use photondb::{env::Photon, raw::Table, TableOptions, TableStats};

use super::Store;
use crate::bench::{Args, Result};

#[derive(Clone)]
pub(crate) struct PhotondbStore {
    table: Table<Photon>,
}

#[async_trait]
impl Store for PhotondbStore {
    async fn open_table(config: Arc<Args>, env: &Photon) -> Self {
        let mut options = TableOptions::default();
        options.page_store.write_buffer_capacity = 128 << 20;
        options.page_store.prepopulate_cache_on_flush = false;
        options.page_store.cache_capacity = 128 << 20;
        options.page_store.cache_estimated_entry_charge = 2511;
        let table = Table::open(env.to_owned(), &config.db, options)
            .await
            .expect("open table fail");
        Self { table }
    }

    async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        self.table.put(key, lsn, value).await.expect("put fail");
        Ok(())
    }

    async fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>> {
        let r = self.table.get(key, lsn).await.expect("get fail");
        Ok(r)
    }

    fn stats(&self) -> Option<TableStats> {
        Some(self.table.stats())
    }
}
