mod map;
pub use map::{Map, Options};

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
    use super::*;
    use crate::util::RelaxedCounter;

    const OPTIONS: Options = Options {
        cache_size: usize::MAX,
        data_node_size: 256,
        data_delta_length: 4,
        index_node_size: 128,
        index_delta_length: 4,
    };

    static SEQUENCE: RelaxedCounter = RelaxedCounter::new(0);

    fn init() {
        let _ = env_logger::builder().try_init();
    }

    fn open(opts: Options) -> Map {
        init();
        Map::open(opts).unwrap()
    }

    fn get(map: &Map, i: usize, should_exists: bool) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.get();
        let expect = if should_exists { Some(key) } else { None };
        map.get(key, lsn, |got| {
            assert_eq!(got, expect);
        })
        .unwrap();
    }

    fn put(map: &Map, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.inc();
        map.put(key, lsn, key).unwrap();
        map.get(key, lsn, |got| {
            assert_eq!(got.unwrap(), key);
        })
        .unwrap();
    }

    fn delete(map: &Map, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.inc();
        map.delete(key, lsn).unwrap();
        map.get(key, lsn, |got| {
            assert_eq!(got, None);
        })
        .unwrap();
    }

    #[test]
    fn crud() {
        let map = open(OPTIONS);
        get(&map, 0, false);
        put(&map, 0);
        delete(&map, 0);
    }

    const N: usize = 1 << 10;
    const M: usize = 1 << 4;

    #[test]
    fn crud_repeated() {
        let map = open(OPTIONS);
        for _ in 0..2 {
            for i in 0..N {
                put(&map, i);
            }
            for i in 0..N {
                get(&map, i, true);
            }
            for i in (0..N).step_by(M) {
                delete(&map, i);
            }
            for i in (0..N).rev() {
                put(&map, i);
            }
            for i in (0..N).rev() {
                get(&map, i, true);
            }
            for i in (0..N).rev().step_by(M) {
                delete(&map, i);
            }
        }
    }

    #[test]
    fn iter() {
        let map = open(OPTIONS);
        for i in 0..N {
            put(&map, i);
        }
        let mut i = 0usize;
        let mut iter = map.iter();
        iter.next_with(|item| {
            let buf = i.to_be_bytes();
            let key = buf.as_slice();
            assert_eq!(item, (key, key));
            i += 1;
        })
        .unwrap();
        assert_eq!(i, N);
    }
}
