#![feature(test)]

mod table;
pub use table::Table;

mod tree;
use tree::{Options, Tree};

mod page;
use page::{
    BaseData, BaseIndex, DeltaData, DeltaIndex, MergeNode, PageBuf, PageContent, PageHandle,
    PageIndex, PageRef, SplitNode,
};

mod pagecache;
use pagecache::{PageCache, PageId};

mod pagetable;
use pagetable::PageTable;

mod store;

#[cfg(test)]
mod test {
    extern crate test;

    use test::Bencher;

    use super::*;

    #[test]
    fn test_basic() {
        let table = Table::default();
        table.put(b"key", b"value");
        assert_eq!(table.get(b"key"), Some(b"value".to_vec()));
        assert_eq!(table.get(b"not_found"), None);
        table.delete(b"key");
        assert_eq!(table.get(b"key"), None);
    }

    #[test]
    fn test_smo() {
        let opts = Options {
            node_split_size: 64,
            node_merge_size: 16,
            delta_chain_length: 4,
        };
        let table = Table::new(opts);

        let n = 4096u64;
        for i in 0..n {
            let key = &i.to_be_bytes();
            table.put(key, key);
            assert_eq!(table.get(key), Some(key.to_vec()));
        }
        for i in 0..n {
            let key = &i.to_be_bytes();
            assert_eq!(table.get(key), Some(key.to_vec()));
        }
    }

    #[test]
    fn multi_thread() {
        const NUM_THREADS: usize = 8;
        const NUM_RECORDS: usize = 1 << 13;

        let opts = Options {
            node_split_size: 64,
            node_merge_size: 16,
            delta_chain_length: 4,
        };
        let table = Table::new(opts);
        let mut handles = Vec::new();
        for t in 0..NUM_THREADS {
            {
                let table = table.clone();
                let handle = std::thread::spawn(move || {
                    for i in 0..NUM_RECORDS {
                        if i % 1024 == 0 {
                            println!("thread {} put {}", t, i);
                        }
                        let key = &i.to_be_bytes();
                        table.put(key, key);
                    }
                });
                handles.push(handle);
            }
            /*
            {
                let table = table.clone();
                let handle = std::thread::spawn(move || {
                    for i in 0..NUM_RECORDS {
                        if i % 1024 == 0 {
                            println!("thread {} get {}", t, i);
                        }
                        let key = &i.to_be_bytes();
                        table.get(key);
                    }
                });
                handles.push(handle);
            }
            */
        }
        for h in handles {
            h.join().unwrap();
        }
        for i in 0..NUM_RECORDS {
            let key = &i.to_be_bytes();
            assert_eq!(table.get(key), Some(key.to_vec()));
        }
    }

    const N: u64 = 1 << 20;
    const M: u64 = 1 << 10;
    const STEP: u64 = N / M;

    #[bench]
    fn bench_put(bench: &mut Bencher) {
        let table = Table::default();
        for i in 0..N {
            let key = &i.to_be_bytes();
            table.put(key, key);
            assert_eq!(table.get(key), Some(key.to_vec()));
        }

        bench.iter(|| {
            let mut i = 0;
            while i < N {
                let key = &i.to_be_bytes();
                table.put(key, key);
                i += STEP;
            }
        });
    }

    #[bench]
    fn bench_get(bench: &mut Bencher) {
        let table = Table::default();
        for i in 0..N {
            let key = &i.to_be_bytes();
            table.put(key, key);
            assert_eq!(table.get(key), Some(key.to_vec()));
        }

        bench.iter(|| {
            let mut i = 0;
            while i < N {
                let key = &i.to_be_bytes();
                table.get(key).unwrap();
                i += STEP;
            }
        });
    }
}
