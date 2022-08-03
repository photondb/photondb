use super::{BTree, Ghost, Options, Result, Stats};

pub struct Table {
    tree: BTree,
}

impl Table {
    pub fn open(opts: Options) -> Result<Self> {
        let tree = BTree::open(opts)?;
        Ok(Self { tree })
    }

    pub fn stats(&self) -> Stats {
        self.tree.stats()
    }

    pub fn get(&self, key: &[u8], lsn: u64) -> Result<Option<Vec<u8>>> {
        let ghost = &Ghost::pin();
        let value = self.tree.get(key, lsn, ghost)?;
        Ok(value.map(|v| v.to_vec()))
    }

    pub fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let ghost = &Ghost::pin();
        self.tree.put(key, lsn, value, ghost)?;
        Ok(())
    }

    pub fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let ghost = &Ghost::pin();
        self.tree.delete(key, lsn, ghost)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init() {
        let _ = env_logger::builder().try_init();
    }

    fn open_small_table() -> Table {
        init();
        let opts = Options {
            data_node_size: 256,
            data_delta_length: 4,
            index_node_size: 128,
            index_delta_length: 4,
            ..Default::default()
        };
        Table::open(opts).unwrap()
    }

    fn open_default_table() -> Table {
        init();
        Table::open(Options::default()).unwrap()
    }

    fn table_get(table: &Table, i: u64, should_exists: bool) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let got = table.get(key, i).unwrap();
        if should_exists {
            assert_eq!(got.unwrap(), key);
        } else {
            assert_eq!(got, None);
        }
    }

    fn table_put(table: &Table, i: u64) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        table.put(key, 0, key).unwrap();
        let got = table.get(key, 0).unwrap();
        assert_eq!(got.unwrap(), key);
    }

    fn table_delete(table: &Table, i: u64) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        table.delete(key, 0).unwrap();
        let got = table.get(key, 0).unwrap();
        assert_eq!(got, None);
    }

    #[test]
    fn crud() {
        let table = open_small_table();
        table_get(&table, 0, false);
        table_put(&table, 0);
        table_delete(&table, 0);
    }

    const N: u64 = 1 << 12;
    const M: u64 = 1 << 4;
    const STEP: usize = (N / M) as usize;

    #[test]
    fn put_forward() {
        let table = open_small_table();
        for i in 0..N {
            table_put(&table, i);
        }
        for i in 0..N {
            table_get(&table, i, true);
        }
    }

    #[test]
    fn put_backward() {
        let table = open_small_table();
        for i in (0..N).rev() {
            table_put(&table, i);
        }
        for i in (0..N).rev() {
            table_get(&table, i, true);
        }
    }

    #[test]
    fn put_repeated() {
        let table = open_default_table();
        for i in 0..N {
            table_put(&table, i);
        }
        for _ in 0..STEP {
            for i in (0..N).step_by(STEP) {
                table_put(&table, i);
            }
        }
    }

    #[test]
    fn put_and_then_get() {
        let table = open_default_table();
        for i in 0..N {
            table_put(&table, i);
        }
        for i in 0..N {
            table_get(&table, i, true);
        }
    }
}
