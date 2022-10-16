use super::Result;
use crate::page::{PageBuf, PageRef};

pub(crate) struct PageTxn;

impl PageTxn {
    pub(crate) fn alloc_id(&self) -> u64 {
        todo!()
    }

    pub(crate) fn dealloc_id(&self, id: u64) {
        todo!()
    }

    pub(crate) fn alloc_page(&self, size: usize) -> Result<(u64, PageBuf<'_>)> {
        todo!()
    }

    pub(crate) fn dealloc_page(&self, addr: u64) {
        todo!()
    }

    pub(crate) async fn read_page(&self, addr: u64) -> Result<PageRef<'_>> {
        todo!()
    }

    pub(crate) fn page_addr(&self, id: u64) -> u64 {
        todo!()
    }

    pub(crate) fn update_page(&self, id: u64, old: u64, new: u64) -> Result<()> {
        // TODO: ensures that old < new so that we can recover the page table in order.
        todo!()
    }

    pub(crate) fn replace_page(&self, id: u64, old: u64, new: u64) -> Result<()> {
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
