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
        data_node_size: 128,
        data_delta_length: 2,
        index_node_size: 32,
        index_delta_length: 2,
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

    fn iter(map: &Map, start: usize, end: usize, step: usize) {
        let mut i = start;
        let mut iter = map.iter();
        iter.next_with(|item| {
            let buf = i.to_be_bytes();
            let key = buf.as_slice();
            assert_eq!(item, (key, key));
            i += step;
        })
        .unwrap();
        assert_eq!(i, end);
    }

    fn put(map: &Map, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let lsn = SEQUENCE.inc();
        map.put(key, lsn, key).unwrap();
        map.get(key, lsn, |got| {
            assert_eq!(got, Some(key));
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

    #[cfg(miri)]
    const N: usize = 1 << 4;
    #[cfg(not(miri))]
    const N: usize = 1 << 10;

    #[test]
    fn crud() {
        let map = open(OPTIONS);
        for _ in 0..2 {
            for i in 0..N {
                put(&map, i);
            }
            for i in 0..N {
                get(&map, i, true);
            }
            iter(&map, 0, N, 1);
            for i in (1..N).step_by(2) {
                delete(&map, i);
            }
            iter(&map, 0, N, 2);
        }
    }

    #[test]
    fn concurrent_crud() {
        let map = open(OPTIONS);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let map = map.clone();
            let handle = std::thread::spawn(move || {
                for i in 0..N {
                    put(&map, i);
                }
                for i in 0..N {
                    get(&map, i, true);
                }
                iter(&map, 0, N, 1);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
