mod table;
pub use table::{Options, Table};

mod error;
pub use error::{Error, Result};

mod stats;
pub use stats::Stats;

mod node;
mod page;
mod pagecache;
mod pagestore;
mod pagetable;

#[cfg(test)]
mod tests {
    use super::{stats::Counter, *};

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

    static SEQUENCE: Counter = Counter::new(0);

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
        let lsn = SEQUENCE.inc();
        table.put(key, lsn, key).unwrap();
        let got = table.get(key, lsn).unwrap();
        assert_eq!(got.unwrap(), key);
    }

    fn table_delete(table: &Table, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.inc();
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
