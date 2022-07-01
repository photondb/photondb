#![feature(test)]

/*
mod table;
pub use table::Table;

mod tree;
use tree::{Config, Tree};

mod page;
use page::{BasePage, DeltaPage, PageContent, SplitPage};

mod pagecache;
use pagecache::{Page, PageCache, PageHeader, PageRef};
*/

mod pagetable;
use pagetable::PageTable;
