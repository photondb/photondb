use super::{tree::Tree, Ghost, Options, ReadOptions, Result};

pub struct Table {
    tree: Tree,
}

impl Table {
    pub async fn open(opts: Options) -> Result<Self> {
        let tree = Tree::open(opts).await?;
        Ok(Self { tree })
    }

    pub async fn get(&self, key: &[u8], opts: &ReadOptions) -> Result<Option<Vec<u8>>> {
        let ghost = &Ghost::pin();
        let value = self.tree.get(key, opts, ghost).await?;
        Ok(value.map(|v| v.to_vec()))
    }

    pub async fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let ghost = &Ghost::pin();
        self.tree.put(key, lsn, value, ghost).await?;
        Ok(())
    }

    pub async fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let ghost = &Ghost::pin();
        self.tree.delete(key, lsn, ghost).await?;
        Ok(())
    }
}
