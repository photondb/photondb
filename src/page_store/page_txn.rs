use std::rc::Rc;

use super::{version::Version, write_buffer::RecordHeader, PageTable, Result};
use crate::page::{PageBuf, PageRef};

pub(crate) struct Guard {
    version: Rc<Version>,
    page_table: PageTable,
}

impl Guard {
    pub fn new(version: Rc<Version>, page_table: PageTable) -> Self {
        Guard {
            version,
            page_table,
        }
    }

    pub(crate) fn begin(&self) -> PageTxn<'_> {
        PageTxn {
            guard: self,
            file_id: self.version.active_write_buffer_id(),
            records: Vec::default(),
            deallocated_ids: Vec::default(),
        }
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

    file_id: u32,
    records: Vec<&'a mut RecordHeader>,
    deallocated_ids: Vec<u64>,
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

    pub(crate) fn update_page(&mut self, id: u64, old: u64, new: u64) -> Result<(), u64> {
        // TODO: ensures that old < new so that we can recover the page table in order.
        todo!()
    }

    pub(crate) fn replace_page(&mut self, id: u64, old: u64, new: u64) -> Result<(), u64> {
        // TODO: ensures that old < new so that we can recover the page table in order.
        todo!()
    }

    pub(crate) fn commit(self) {}
}

impl<'a> Drop for PageTxn<'a> {
    fn drop(&mut self) {
        // abort the transaction
        // 1. mark all records as tombstone
        // 2. cas update write buffer num_writers.
        // 3. return allocated node ids.
        todo!()
    }
}
