use super::{BTree, Ghost, Options, Result};

pub struct Table {
    tree: BTree,
}

impl Table {
    pub async fn open(opts: Options) -> Result<Self> {
        let tree = BTree::open(opts).await?;
        Ok(Self { tree })
    }

    pub async fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>> {
        let ghost = &Ghost::pin();
        let value = self.tree.get(key, lsn, ghost).await?;
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

#[cfg(test)]
mod test {
    use super::*;

    async fn open_table() -> Table {
        let opts = Options {
            data_node_size: 256,
            data_delta_length: 4,
            index_node_size: 128,
            index_delta_length: 4,
            ..Default::default()
        };
        Table::open(opts).await.unwrap()
    }

    #[tokio::test]
    async fn small_dataset() {
        let table = open_table().await;
        let key = b"key";
        let value = b"value";
        table.put(key, 0, value).await.unwrap();
        let got_value = table.get(key, 0).await.unwrap().unwrap();
        assert_eq!(got_value, value);
        table.delete(key, 0).await.unwrap();
        let got_value = table.get(key, 0).await.unwrap();
        assert_eq!(got_value, None);
    }

    #[tokio::test]
    async fn large_dataset() {
        const N: u64 = 1024;
        let table = open_table().await;
        for i in 0..N {
            let buf = i.to_be_bytes();
            let key = buf.as_slice();
            let value = buf.as_slice();
            table.put(key, i, value).await.unwrap();
            let got_value = table.get(key, i).await.unwrap().unwrap();
            assert_eq!(got_value, value);
        }
        for i in 0..N {
            let buf = i.to_be_bytes();
            let key = buf.as_slice();
            let value = buf.as_slice();
            let got_value = table.get(key, i).await.unwrap().unwrap();
            assert_eq!(got_value, value);
        }
    }
}
