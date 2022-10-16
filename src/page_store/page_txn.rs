use super::{PageAddr, PageId, Result};
use crate::page::{PageBuf, PageRef};

pub(crate) struct PageTxn;

impl PageTxn {
    pub(crate) fn alloc_id(&self) -> PageId {
        todo!()
    }

    pub(crate) fn dealloc_id(&self, id: PageId) {
        todo!()
    }

    pub(crate) fn alloc_page(&self, size: usize) -> Result<(PageAddr, PageBuf<'_>)> {
        todo!()
    }

    pub(crate) fn dealloc_page(&self, addr: PageAddr) {
        todo!()
    }

    pub(crate) async fn read_page(&self, addr: PageAddr) -> Result<PageRef<'_>> {
        todo!()
    }

    pub(crate) fn page_addr(&self, id: PageId) -> PageAddr {
        todo!()
    }

    pub(crate) fn update_page(
        &self,
        id: PageId,
        old: PageAddr,
        new: PageAddr,
    ) -> std::result::Result<(), PageAddr> {
        // TODO: ensures that old < new so that we can recover the page table in order.
        todo!()
    }

    pub(crate) fn commit(self) {}
}

impl Drop for PageTxn {
    fn drop(&mut self) {
        // abort the transaction
        todo!()
    }
}
