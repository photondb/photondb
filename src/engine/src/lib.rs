#![feature(test)]

mod table;
pub use table::Table;

mod tree;
use tree::{Options, Tree};

mod page;
use page::{
    BaseData, BaseIndex, DeltaData, DeltaIndex, MergeNode, PageBuf, PageContent, PageHeader,
    PageIndex, PageRef, SplitNode,
};

mod pagecache;
use pagecache::{PageCache, PageId};

mod pagetable;
use pagetable::PageTable;

#[cfg(test)]
mod test {
    extern crate test;

    use test::{black_box, Bencher};

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
            data_node_size: 128,
            index_node_size: 128,
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
