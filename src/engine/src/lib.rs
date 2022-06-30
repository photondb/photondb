#![feature(test)]

mod page;
use page::{BaseData, DeltaData, Page, PageContent, PageHeader};

mod pagecache;
use pagecache::{PageCache, PageRef};

mod pagetable;
use pagetable::{PageId, PageTable};
