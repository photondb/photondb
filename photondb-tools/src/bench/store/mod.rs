use std::sync::Arc;

use ::photondb::TableStats;
use async_trait::async_trait;

mod photondb;

pub(crate) use self::photondb::PhotondbStore;
use super::*;

#[async_trait]
pub(crate) trait Store<E>: Clone + Sync + Send + 'static {
    async fn open_table(config: Arc<Args>, env: &E) -> Self;

    async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()>;

    async fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>>;

    fn stats(&self) -> Option<TableStats>;
}
