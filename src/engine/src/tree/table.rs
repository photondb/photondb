use super::{Ghost, Options, Tree};
use crate::Result;

pub struct Table {
    tree: Tree,
}

impl Table {
    pub async fn open(opts: Options) -> Result<Self> {
        let tree = Tree::open(opts).await?;
        Ok(Self { tree })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ghost = &Ghost::pin();
        let value = self.tree.get(key, ghost).await?;
        Ok(value.map(|v| v.to_vec()))
    }

    pub async fn put(&self, lsn: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let ghost = &Ghost::pin();
        self.tree.put(lsn, key, value, ghost).await?;
        Ok(())
    }

    pub async fn delete(&self, lsn: u64, key: &[u8]) -> Result<()> {
        let ghost = &Ghost::pin();
        self.tree.delete(lsn, key, ghost).await?;
        Ok(())
    }
}
