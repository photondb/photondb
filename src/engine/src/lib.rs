#![feature(test)]

/*
mod table;
pub use table::Table;

mod tree;
use tree::{Config, Tree};
*/

mod pagecache;
use pagecache::{
    BasePage, DeltaPage, PageCache, PageContent, PageHeader, PageId, PageRef, SplitPage,
};

mod pagetable;
use pagetable::PageTable;
