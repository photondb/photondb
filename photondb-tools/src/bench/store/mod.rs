use std::sync::Arc;

use ::photondb::{env::Photon, StoreStats, TreeStats};
use async_trait::async_trait;

mod photondb;

pub(crate) use self::photondb::PhotondbStore;
use super::*;

#[async_trait]
pub(crate) trait Store: Clone + Sync + Send + 'static {
    async fn open_table(config: Arc<Args>, _env: &Photon) -> Self;

    async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()>;

    async fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>>;

    fn stats(&self) -> Option<(TreeStats, StoreStats)>;
}
