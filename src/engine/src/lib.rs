#![feature(test)]

/*
mod table;
pub use table::Table;

mod tree;
use tree::{Config, Tree};
*/

mod page;
use page::{BaseData, DeltaData, OwnedPage, Page, PageContent, PageHeader, SharedPage, SplitPage};

mod pagecache;
use pagecache::{PageCache, PageId};

mod pagetable;
use pagetable::PageTable;
