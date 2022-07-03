#![feature(test)]

/*
mod table;
pub use table::Table;

mod tree;
use tree::{Config, Tree};
*/

mod page;
use page::{BaseData, DeltaData, Page, PageContent, PageHeader, SplitPage};

mod pagecache;
use pagecache::{PageCache, PageId};

mod pagetable;
use pagetable::PageTable;
