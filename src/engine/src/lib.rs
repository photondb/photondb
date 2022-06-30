#![feature(test)]

mod table;
pub use table::Table;

mod tree;
use tree::Tree;

mod page;
use page::{BaseData, DeltaData, PageContent};

mod pagecache;
use pagecache::{Page, PageCache, PageHeader, PageRef};

mod pagetable;
use pagetable::{PageId, PageTable};
