use std::{rc::Rc, sync::Arc};

use super::{
    version::Version,
    write_buffer::{RecordHeader, ReleaseState},
    PageTable, Result, WriteBuffer,
};
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

/// A transaction to manipulate pages in a page store.
///
/// On drop, the transaction will be aborted and all its operations will be
/// rolled back.
pub(crate) struct PageTxn<'a> {
    guard: &'a Guard,

    file_id: u32,
    records: Vec<&'a mut RecordHeader>,
    deallocated_ids: Vec<u64>,
}

impl<'a> PageTxn<'a> {
    /// Allocates a page buffer with the given size.
    ///
    /// Returns the address and buffer of the allocated page.
    ///
    /// If the transaction aborts, all pages allocated by this transaction will
    /// be deallocated.
    pub(crate) fn alloc_page(&mut self, size: usize) -> Result<(u64, PageBuf<'a>)> {
        todo!()
    }

    /// Inserts a new page into the store.
    ///
    /// Returns the id of the inserted page.
    ///
    /// If the transaction aborts, the inserted page will be deleted.
    pub(crate) fn insert_page(&mut self, addr: u64) -> u64 {
        todo!()
    }

    /// Updates the page address to `new_addr` if its current value is the same
    /// as `old_addr`.
    ///
    /// On success, commits all operations in the transaction.
    /// On failure, returns the transaction and the current address of the page.
    pub(crate) fn update_page(
        self,
        id: u64,
        old_addr: u64,
        new_addr: u64,
    ) -> Result<(), (Self, u64)> {
        // TODO: ensure that old_addr < new_addr so that we can recover the page table
        // in order.
        todo!()
    }

    /// This function is similar to [`Self::update_page`], except that it also
    /// deallocates some pages on success.
    ///
    /// The deallocated pages will still be valid until no one is able to access
    /// them.
    pub(crate) fn replace_page(
        self,
        id: u64,
        old_addr: u64,
        new_addr: u64,
        dealloc_addrs: &[u64],
    ) -> Result<(), (Self, u64)> {
        // TODO: ensure that old_addr < new_addr so that we can recover the page table
        // in order.
        todo!()
    }

    fn seal_write_buffer(&mut self) -> Result<()> {
        let release_state = {
            let buffer_set = self.guard.version.buffer_set.current();
            let write_buffer = buffer_set
                .write_buffer(self.file_id)
                .expect("The memory buffer should exists");
            // TODO: add safety condition
            unsafe { write_buffer.seal(false)? }
        };

        self.file_id += 1;
        let capacity = self.guard.version.buffer_set.write_buffer_capacity();
        let write_buffer = Arc::new(WriteBuffer::with_capacity(self.file_id, capacity));
        self.guard.version.buffer_set.install(write_buffer);
        if matches!(release_state, ReleaseState::Flush) {
            self.guard.version.buffer_set.notify_flush_job();
        }
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_store::{page_table::PageTable, version::Version};

    #[test]
    #[ignore] // ignore since `PageTxn::drop` is not implemented.
    fn page_txn_seal_write_buffer() {
        let version = Rc::new(Version::new(512));
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), page_table);
        let mut page_txn = guard.begin();
        page_txn.seal_write_buffer().unwrap();
    }
}
