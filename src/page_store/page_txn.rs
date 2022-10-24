use std::{collections::HashMap, rc::Rc, sync::Arc};

use super::{
    version::Version,
    write_buffer::{RecordHeader, ReleaseState},
    Error, PageTable, Result, WriteBuffer, NAN_ID,
};
use crate::page::{PageBuf, PageRef};

pub(crate) struct Guard {
    version: Rc<Version>,
    page_table: PageTable,
}

impl Guard {
    pub(crate) fn new(version: Rc<Version>, page_table: PageTable) -> Self {
        Guard {
            version,
            page_table,
        }
    }

    pub(crate) fn begin(&self) -> PageTxn {
        let file_id = loop {
            let current = self.version.buffer_set.current();
            let writer_buffer = current.last_writer_buffer();
            if writer_buffer.is_sealed() {
                continue;
            }
            break writer_buffer.file_id();
        };

        PageTxn {
            guard: self,
            file_id,
            records: HashMap::default(),
            page_ids: Vec::default(),
            deleted_page: None,
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
    records: HashMap<u64 /* page addr */, &'a mut RecordHeader>,
    page_ids: Vec<u64>,
    deleted_page: Option<&'a mut RecordHeader>,
}

impl<'a> PageTxn<'a> {
    /// Allocates a page buffer with the given size.
    ///
    /// Returns the address and buffer of the allocated page.
    ///
    /// If the transaction aborts, all pages allocated by this transaction will
    /// be deallocated.
    pub(crate) fn alloc_page(&mut self, size: usize) -> Result<(u64, PageBuf<'a>)> {
        let page_size = size as u32;
        let (addr, header, buf) = self.alloc_page_impl(page_size)?;
        self.records.insert(addr, header);
        Ok((addr, buf))
    }

    /// Inserts a new page into the store.
    ///
    /// Returns the id of the inserted page.
    ///
    /// If the transaction aborts, the inserted page will be deleted.
    pub(crate) fn insert_page(&mut self, addr: u64) -> u64 {
        let header = self.records.get_mut(&addr).expect("no such pages");
        if header.is_tombstone() {
            panic!("insert page with tombstone");
        }

        // TODO: safety conditions
        let page_id = unsafe { self.guard.page_table.alloc() }.expect("page id is exhausted");
        self.guard.page_table.set(page_id, addr);

        header.set_page_id(page_id);
        self.page_ids.push(page_id);
        page_id
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
        assert!(old_addr < new_addr);
        if let Err(addr) = self.guard.page_table.cas(id, old_addr, new_addr) {
            return Err((self, addr));
        }

        // FIXME: should update page id of new_addr?

        self.commit();
        Ok(())
    }

    /// This function is similar to [`Self::update_page`], except that it also
    /// deallocates some pages on success.
    ///
    /// The deallocated pages will still be valid until no one is able to access
    /// them.
    pub(crate) fn replace_page(
        mut self,
        id: u64,
        old_addr: u64,
        new_addr: u64,
        dealloc_addrs: &[u64],
    ) -> Result<()> {
        assert!(old_addr < new_addr);
        self.deleted_page = Some(self.dealloc_pages_impl(dealloc_addrs)?);
        self.update_page(id, old_addr, new_addr)
            .map_err(|_| Error::Again)?;
        Ok(())
    }

    fn seal_write_buffer(&mut self) {
        let release_state = {
            let buffer_set = self.guard.version.buffer_set.current();
            let write_buffer = buffer_set
                .write_buffer(self.file_id)
                .expect("The memory buffer should exists");
            // TODO: add safety condition
            match unsafe { write_buffer.seal(false) } {
                Ok(release_state) => release_state,
                Err(_) => {
                    // The write buffer has been sealed.
                    return;
                }
            }
        };

        self.file_id += 1;
        let capacity = self.guard.version.buffer_set.write_buffer_capacity();
        let write_buffer = Arc::new(WriteBuffer::with_capacity(self.file_id, capacity));
        self.guard.version.buffer_set.install(write_buffer);
        if matches!(release_state, ReleaseState::Flush) {
            self.guard.version.buffer_set.notify_flush_job();
        }
    }

    #[inline]
    fn is_first_op(&self) -> bool {
        self.records.is_empty()
    }

    #[inline]
    fn alloc_page_impl(
        &mut self,
        page_size: u32,
    ) -> Result<(u64, &'a mut RecordHeader, PageBuf<'a>)> {
        let is_first_op = self.is_first_op();
        self.with_write_buffer(|write_buffer| unsafe {
            // Safety: [`guard`] guarantees the lifetime of the page reference.
            write_buffer.alloc_page(NAN_ID, page_size, is_first_op)
        })
    }

    #[inline]
    fn dealloc_pages_impl(&mut self, page_addrs: &[u64]) -> Result<&'a mut RecordHeader> {
        let is_first_op = self.is_first_op();
        self.with_write_buffer(|write_buffer| unsafe {
            // Safety: [`guard`] guarantees the lifetime of the page reference.
            write_buffer.dealloc_pages(page_addrs, is_first_op)
        })
    }

    #[inline]
    fn with_write_buffer<F, O>(&mut self, f: F) -> Result<O>
    where
        F: FnOnce(&WriteBuffer) -> Result<O>,
    {
        self.guard
            .version
            .with_write_buffer(self.file_id, f)
            .map_err(|err| {
                assert!(matches!(err, Error::Again));
                self.seal_write_buffer();
                err
            })
    }

    #[inline]
    fn drop_writer_guard(&mut self) {
        let release_state =
            self.guard
                .version
                .with_write_buffer(self.file_id, |write_buffer| unsafe {
                    // Safety: all mutable references are released.
                    write_buffer.release_writer()
                });
        if matches!(release_state, ReleaseState::Flush) {
            self.guard.version.buffer_set.notify_flush_job();
        }
    }

    fn commit(mut self) {
        if !self.records.is_empty() {
            self.drop_writer_guard();
        }

        self.records.clear();
        self.deleted_page = None;
    }
}

impl<'a> Drop for PageTxn<'a> {
    fn drop(&mut self) {
        let mut has_writer_guard = !self.records.is_empty();
        for (_, header) in &mut self.records {
            header.set_tombstone();
        }
        for id in &self.page_ids {
            // TODO: safety conditions.
            unsafe { self.guard.page_table.dealloc(*id) };
        }

        if let Some(deleted_pages) = &mut self.deleted_page {
            deleted_pages.set_tombstone();
            has_writer_guard = true;
        }

        self.records.clear();
        self.deleted_page = None;

        if has_writer_guard {
            self.guard
                .version
                .with_write_buffer(self.file_id, |write_buffer| {
                    // Safety: all mutable references are released.
                    unsafe { write_buffer.release_writer() };
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_store::{page_table::PageTable, version::Version};

    #[test]
    fn page_txn_update_page() {
        let version = Rc::new(Version::new(512));
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), page_table);
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).unwrap();
        assert!(page_txn.update_page(id, addr, new).is_ok());
    }

    #[test]
    fn page_txn_failed_update_page() {
        let version = Rc::new(Version::new(1 << 10));
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), page_table);

        // insert old page.
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).unwrap();
        assert!(page_txn.update_page(id, addr, new).is_ok());

        // operate is failed.
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(12).unwrap();
        assert!(page_txn.update_page(id, addr, 0).is_err());
    }

    #[test]
    fn page_txn_replace_page() {
        let version = Rc::new(Version::new(1 << 10));
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), page_table);
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).unwrap();
        assert!(page_txn.replace_page(id, addr, new, &[1, 2, 3]).is_ok());
    }

    #[test]
    fn page_txn_seal_write_buffer() {
        let version = Rc::new(Version::new(512));
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), page_table);
        let mut page_txn = guard.begin();
        page_txn.seal_write_buffer();
    }
}
