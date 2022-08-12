mod error;
pub use error::{Error, Result};

mod map;
pub use map::{Map, RawMap};

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

    fn open(opts: Options) -> Map {
        init();
        Map::open(opts).unwrap()
    }

    fn get(map: &Map, i: usize, should_exists: bool) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        let expect = if should_exists { Some(key) } else { None };
        map.get(key, |got| {
            assert_eq!(got, expect);
        })
        .unwrap();
    }

    fn iter(map: &Map, start: usize, end: usize, step: usize) {
        let mut iter = map.iter();
        for _ in 0..2 {
            let mut i = start;
            iter.reset();
            iter.next_with(|item| {
                let buf = i.to_be_bytes();
                let key = buf.as_slice();
                assert_eq!(item, (key, key));
                i += step;
            })
            .unwrap();
            assert_eq!(i, end);
        }
    }

    fn put(map: &Map, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        map.put(key, key).unwrap();
        map.get(key, |got| {
            assert_eq!(got, Some(key));
        })
        .unwrap();
    }

    fn delete(map: &Map, i: usize) {
        let buf = i.to_be_bytes();
        let key = buf.as_slice();
        map.delete(key).unwrap();
        map.get(key, |got| {
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
        for _ in 0..T {
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
