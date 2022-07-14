mod table;
pub use table::Table;

mod error;
use error::{Error, Result};

mod btree;
use btree::BTree;

mod page;
use page::{PageAddr, PageKind, PageRef, PageView};

mod pagetable;
use pagetable::PageTable;

mod pagestore;
use pagestore::{DiskAddr, DiskHandle, PageDesc, PageInfo, PageStore};

pub struct Ghost {
    guard: crossbeam_epoch::Guard,
}

impl Ghost {
    pub fn pin() -> Self {
        let guard = crossbeam_epoch::pin();
        Self { guard }
    }
}

pub struct Options {
    pub data_node_size: usize,
    pub data_delta_length: usize,
    pub index_node_size: usize,
    pub index_delta_length: usize,
}
