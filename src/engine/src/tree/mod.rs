mod error;
pub use error::{Error, Result};

mod table;
pub use table::{RawTable, Table};

mod stats;
pub use stats::Stats;

mod node;
mod page;
mod pagecache;
mod pagestore;
mod pagetable;
#[allow(clippy::module_inception)]
mod tree;

#[derive(Clone, Debug)]
pub struct Options {
    pub cache_size: usize,
    pub data_node_size: usize,
    pub data_delta_length: usize,
    pub index_node_size: usize,
    pub index_delta_length: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_size: usize::MAX,
            data_node_size: 8 * 1024,
            data_delta_length: 8,
            index_node_size: 4 * 1024,
            index_delta_length: 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPTIONS: Options = Options {
        cache_size: usize::MAX,
        data_node_size: 128,
        data_delta_length: 4,
        index_node_size: 64,
        index_delta_length: 3,
    };

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
        let expect = if should_exists { Some(key) } else { None };
        table
            .get(key, |got| {
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
        table.put(key, key).unwrap();
        table
            .get(key, |got| {
                assert_eq!(got, Some(key));
            })
            .unwrap();
    }

    fn delete(table: &Table, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        table.delete(key).unwrap();
        table
            .get(key, |got| {
                assert_eq!(got, None);
            })
            .unwrap();
    }

    #[cfg(miri)]
    const N: usize = 1 << 4;
    #[cfg(miri)]
    const T: usize = 4;
    #[cfg(not(miri))]
    const N: usize = 1 << 10;
    #[cfg(not(miri))]
    const T: usize = 8;

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
        for _ in 0..T {
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
