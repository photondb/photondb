mod error;
pub use error::{Error, Result};

mod stats;
pub use stats::Stats;

mod node;
mod page;
mod pagecache;
mod pagestore;
mod pagetable;

/*
mod table;
pub use table::{Options, Table};


#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::RelaxedCounter;

    const OPTIONS: Options = Options {
        cache_size: usize::MAX,
        data_node_size: 128,
        data_delta_length: 2,
        index_node_size: 32,
        index_delta_length: 2,
    };

    static SEQUENCE: RelaxedCounter = RelaxedCounter::new(0);

    fn init() {
        let _ = env_logger::builder().try_init();
    }

    fn open(opts: Options) -> Table {
        init();
        Table::open(opts).unwrap()
    }

    fn get(table: &Table, i: usize, should_exists: bool) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.get();
        let expect = if should_exists { Some(key) } else { None };
        table
            .get(key, lsn, |got| {
                assert_eq!(got, expect);
            })
            .unwrap();
    }

    fn iter(table: &Table, start: usize, end: usize, step: usize) {
        let mut i = start;
        let mut iter = table.iter();
        iter.next_with(|item| {
            let buf = i.to_be_bytes();
            let key = buf.as_slice();
            assert_eq!(item, (key, key));
            i += step;
        })
        .unwrap();
        assert_eq!(i, end);
    }

    fn put(table: &Table, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.inc();
        table.put(key, lsn, key).unwrap();
        table
            .get(key, lsn, |got| {
                assert_eq!(got, Some(key));
            })
            .unwrap();
    }

    fn delete(table: &Table, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.inc();
        table.delete(key, lsn).unwrap();
        table
            .get(key, lsn, |got| {
                assert_eq!(got, None);
            })
            .unwrap();
    }

    #[cfg(miri)]
    const N: usize = 1 << 4;
    #[cfg(not(miri))]
    const N: usize = 1 << 10;

    #[test]
    fn crud() {
        let table = open(OPTIONS);
        for _ in 0..2 {
            for i in 0..N {
                put(&table, i);
            }
            for i in 0..N {
                get(&table, i, true);
            }
            iter(&table, 0, N, 1);
            for i in (1..N).step_by(2) {
                delete(&table, i);
            }
            iter(&table, 0, N, 2);
        }
    }

    #[test]
    fn concurrent_crud() {
        let table = open(OPTIONS);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let table = table.clone();
            let handle = std::thread::spawn(move || {
                for i in 0..N {
                    put(&table, i);
                }
                for i in 0..N {
                    get(&table, i, true);
                }
                iter(&table, 0, N, 1);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
*/
