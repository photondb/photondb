use super::Result;
use crate::page::{PageBuf, PageRef};

pub(crate) struct Guard;

impl Guard {
    pub(crate) fn begin(&self) -> PageTxn<'_> {
        PageTxn { guard: self }
    }

    pub(crate) fn page_addr(&self, id: u64) -> u64 {
        todo!()
    }

    pub(crate) async fn read_page(&self, addr: u64) -> Result<PageRef<'_>> {
        todo!()
    }
}

pub(crate) struct PageTxn<'a> {
    guard: &'a Guard,
}

impl<'a> PageTxn<'a> {
    pub(crate) fn alloc_id(&mut self) -> u64 {
        todo!()
    }

    pub(crate) fn dealloc_id(&mut self, id: u64) {
        todo!()
    }

    pub(crate) fn alloc_page(&mut self, size: usize) -> Result<(u64, PageBuf<'a>)> {
        todo!()
    }

    pub(crate) fn dealloc_page(&mut self, addr: u64) {
        todo!()
    }

    pub(crate) fn update_page(&mut self, id: u64, old: u64, new: u64) -> Result<()> {
        // TODO: ensures that old < new so that we can recover the page table in order.
        todo!()
    }

    pub(crate) fn replace_page(&mut self, id: u64, old: u64, new: u64) -> Result<()> {
        // TODO: ensures that old < new so that we can recover the page table in order.
        todo!()
    }

    pub(crate) fn commit(self) {}
}

impl<'a> Drop for PageTxn<'a> {
    fn drop(&mut self) {
        // abort the transaction
        todo!()
    }
}
