use std::sync::Arc;

use ::photondb::TableStats;
use async_trait::async_trait;

mod photondb;

pub(crate) use self::photondb::PhotondbStore;
use super::*;

#[async_trait]
pub(crate) trait Store<E>: std::fmt::Debug + Clone + Sync + Send + 'static {
    async fn open_table(config: Arc<Args>, env: &E) -> Self;

    async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()>;

    async fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>>;

    async fn flush(&self);

    async fn wait_for_reclaiming(&self);

    async fn close(self) -> Result<(), Self>;

    fn stats(&self) -> Option<TableStats>;
}
