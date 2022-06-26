mod table;
pub use table::Table;

mod tree;
use tree::Tree;

mod pagecache;
use pagecache::{BaseData, DeltaData, Page, PageCache, PageId, PageKind, PagePtr};

mod pagetable;
use pagetable::PageTable;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let table = Table::new();
        table.put(b"key", b"value");
        assert_eq!(table.get(b"key"), Some(b"value".to_vec()));
        assert_eq!(table.get(b"key2"), None);
        table.delete(b"key");
        assert_eq!(table.get(b"key"), None);
    }
}
