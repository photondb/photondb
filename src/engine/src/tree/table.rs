use crossbeam_epoch::pin;

use super::{BTree, Options, Result, Stats};

pub struct Iter {}

impl Iter {
    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        todo!()
    }

    pub fn seek(&mut self, _target: &[u8]) {
        todo!()
    }
}

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
        let guard = &pin();
        let value = self.tree.get(key, lsn, guard)?;
        Ok(value.map(|v| v.to_vec()))
    }

    pub fn iter(&self) -> Iter {
        Iter {}
    }

    pub fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let guard = &pin();
        self.tree.put(key, lsn, value, guard)?;
        Ok(())
    }

    pub fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let guard = &pin();
        self.tree.delete(key, lsn, guard)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn init() {
        let _ = env_logger::builder().try_init();
    }

    const OPTIONS: Options = Options {
        cache_size: usize::MAX,
        data_node_size: 256,
        data_delta_length: 4,
        index_node_size: 128,
        index_delta_length: 4,
    };

    static SEQUENCE: Sequence = Sequence::new(0);

    struct Sequence(AtomicU64);

    impl Sequence {
        const fn new(v: u64) -> Self {
            Self(AtomicU64::new(v))
        }

        fn get(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }

        fn next(&self) -> u64 {
            self.0.fetch_add(1, Ordering::Relaxed)
        }
    }

    fn open_table(opts: Options) -> Table {
        init();
        Table::open(opts).unwrap()
    }

    fn table_get(table: &Table, i: usize, should_exists: bool) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.get();
        let got = table.get(key, lsn).unwrap();
        if should_exists {
            assert_eq!(got.unwrap(), key);
        } else {
            assert_eq!(got, None);
        }
    }

    fn table_put(table: &Table, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.next();
        table.put(key, lsn, key).unwrap();
        let got = table.get(key, lsn).unwrap();
        assert_eq!(got.unwrap(), key);
    }

    fn table_delete(table: &Table, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.next();
        table.delete(key, lsn).unwrap();
        let got = table.get(key, lsn).unwrap();
        assert_eq!(got, None);
    }

    #[test]
    fn crud() {
        let table = open_table(OPTIONS);
        table_get(&table, 0, false);
        table_put(&table, 0);
        table_delete(&table, 0);
    }

    const N: usize = 1 << 10;
    const M: usize = 1 << 4;

    #[test]
    fn crud_repeated() {
        let table = open_table(OPTIONS);
        for _ in 0..2 {
            for i in 0..N {
                table_put(&table, i);
            }
            for i in 0..N {
                table_get(&table, i, true);
            }
            for i in (0..N).step_by(M) {
                table_delete(&table, i);
            }
            for i in (0..N).rev() {
                table_put(&table, i);
            }
            for i in (0..N).rev() {
                table_get(&table, i, true);
            }
            for i in (0..N).rev().step_by(M) {
                table_delete(&table, i);
            }
        }
    }
}
