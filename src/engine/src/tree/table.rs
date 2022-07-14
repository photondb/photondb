use crossbeam_epoch::pin;

use super::{BTree, Options};
use crate::Result;

pub struct Table {
    tree: BTree,
}

impl Table {
    pub async fn open(opts: Options) -> Result<Self> {
        let tree = BTree::open(opts).await?;
        Ok(Self { tree })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = &pin();
        let value = self.tree.get(key, guard).await?;
        Ok(value.map(|v| v.to_vec()))
    }
}
