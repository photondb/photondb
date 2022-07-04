#![feature(test)]

mod table;
pub use table::Table;

mod tree;
use tree::{Options, Tree};

mod page;
use page::{
    BaseData, DeltaData, MergeNode, OwnedPage, PageContent, PageHeader, SharedPage, SplitNode,
};

mod pagecache;
use pagecache::{PageCache, PageId};

mod pagetable;
use pagetable::PageTable;
