#![feature(test)]

mod table;
pub use table::Table;

mod tree;
use tree::{Options, Tree};

mod page;
use page::{
    BaseData, DeltaData, MergeNode, PageBuf, PageContent, PageHeader, PageIndex, PageRef, SplitNode,
};

mod pagecache;
use pagecache::{PageCache, PageId};

mod pagetable;
use pagetable::PageTable;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
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
        for i in 0..1024u32 {
            let key = &i.to_be_bytes();
            table.put(key, key);
            assert_eq!(table.get(key), Some(key.to_vec()));
        }
    }
}
