use crate::Result;

mod base;
pub(crate) use base::{AtomicPageId, PageAddr, PageId};

mod page_txn;
pub(crate) use page_txn::PageTxn;

mod page_table;
use page_table::PageTable;

pub(crate) struct PageStore {
    table: PageTable,
}

impl PageStore {
    pub(crate) async fn open() -> Result<Self> {
        todo!()
    }

    pub(crate) fn begin(&self) -> PageTxn {
        todo!()
    }
}
