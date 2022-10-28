use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::{
    version::Version,
    write_buffer::{RecordHeader, ReleaseState},
    Error, PageFiles, PageTable, Result, WriteBuffer, NAN_ID,
};
use crate::page::{PageBuf, PageRef};

pub(crate) struct Guard<'a>
where
    Self: Send,
{
    version: Arc<Version>,
    page_table: &'a PageTable,
    page_files: &'a PageFiles,
    owned_pages: Mutex<Vec<Vec<u8>>>,
}

impl<'a> Guard<'a> {
    pub(crate) fn new(
        version: Arc<Version>,
        page_table: &'a PageTable,
        page_files: &'a PageFiles,
    ) -> Self {
        Guard {
            version,
            page_table,
            page_files,
            owned_pages: Mutex::default(),
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
        }
    }

    #[inline]
    pub(crate) fn page_addr(&self, id: u64) -> u64 {
        let page_addr = self.page_table.get(id);
        if page_addr == 0 {
            // All pages are accessed gradually starting from `MIN_ID`. If a page addr is
            // the default value, there must be some concurrency problems or other potential
            // errors.
            panic!("No such page exists");
        }
        page_addr
    }

    pub(crate) async fn read_page(&self, addr: u64) -> Result<PageRef> {
        let file_id = (addr >> 32) as u32;
        if self.version.contains_write_buffer(file_id) {
            let page_ref = self
                .version
                .with_write_buffer(file_id, |write_buffer| unsafe {
                    // Safety: all mutable references are released.
                    write_buffer.page(addr)
                });
            Ok(page_ref)
        } else {
            let Some(file_info) = self.version.files().get(&file_id) else {
                panic!("File {file_id} is not exists");
            };
            let handle = file_info
                .get_page_handle(addr)
                .expect("The addr is not belongs to the target page file");

            // TODO: cache page file reader for speed up.
            let reader = self
                .page_files
                .open_page_reader(file_id, file_info.meta().block_size())
                .await?;
            let mut buf = vec![0u8; handle.size as usize];
            reader.read_exact_at(&mut buf, handle.offset as u64).await?;

            let mut owned_pages = self.owned_pages.lock().expect("Poisoned");
            owned_pages.push(buf);
            let page = owned_pages.last().expect("Verified");
            let page = page.as_slice();

            Ok(PageRef::new(unsafe {
                // Safety: the lifetime is guarranted by `guard`.
                std::slice::from_raw_parts(page.as_ptr(), page.len())
            }))
        }
    }
}

/// A transaction to manipulate pages in a page store.
///
/// On drop, the transaction will be aborted and all its operations will be
/// rolled back.
pub(crate) struct PageTxn<'a>
where
    Self: Send,
{
    guard: &'a Guard<'a>,

    file_id: u32,
    records: HashMap<u64 /* page addr */, &'a mut RecordHeader>,
    page_ids: Vec<u64>,
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
    /// On failure, if `new_addr` is not large than `old_addr`, `Err(None)` is
    /// returned, otherwise returns the transaction and the current address of
    /// the page.
    pub(crate) fn update_page(
        mut self,
        id: u64,
        old_addr: u64,
        new_addr: u64,
    ) -> Result<(), Option<(Self, u64)>> {
        if new_addr <= old_addr {
            return Err(None);
        }

        if let Err(addr) = self.guard.page_table.cas(id, old_addr, new_addr) {
            return Err(Some((self, addr)));
        }

        let record_header = self
            .records
            .get_mut(&new_addr)
            .expect("No such page exists");
        record_header.set_page_id(id);

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
        if new_addr <= old_addr {
            return Err(Error::Again);
        }

        let dealloc_pages = self.dealloc_pages_impl(dealloc_addrs)?;
        self.update_page(id, old_addr, new_addr).map_err(|_| {
            dealloc_pages.set_tombstone();
            Error::Again
        })?;
        Ok(())
    }

    fn seal_write_buffer(&mut self) {
        let release_state = {
            let buffer_set = self.guard.version.buffer_set.current();
            let write_buffer = buffer_set
                .write_buffer(self.file_id)
                .expect("The memory buffer should exists");

            let release_writer = !self.is_first_op();
            // Safety: all references are immutable.
            match unsafe { write_buffer.seal(release_writer) } {
                Ok(release_state) => {
                    self.records.clear();
                    release_state
                }
                Err(_) => {
                    // The write buffer has been sealed.
                    return;
                }
            }
        };

        let capacity = self.guard.version.buffer_set.write_buffer_capacity();
        let write_buffer = Arc::new(WriteBuffer::with_capacity(self.file_id + 1, capacity));
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
            self.records.clear();
        }

        self.page_ids.clear();
    }
}

impl<'a> Drop for PageTxn<'a> {
    fn drop(&mut self) {
        for id in &self.page_ids {
            // TODO: safety conditions.
            unsafe { self.guard.page_table.dealloc(*id) };
        }

        if !self.records.is_empty() {
            for header in self.records.values_mut() {
                header.set_tombstone();
            }
            self.records.clear();
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
    use std::collections::HashSet;

    use super::*;
    use crate::page_store::{page_table::PageTable, version::Version};

    fn new_version(size: u32) -> Arc<Version> {
        Arc::new(Version::new(size, 1, HashMap::default(), HashSet::new()))
    }

    #[test]
    fn page_txn_update_page() {
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(&base, "test_page_txn_update_page"))
        };

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, &page_table, &files);
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).unwrap();
        assert!(page_txn.update_page(id, addr, new).is_ok());
    }

    #[test]
    fn page_txn_failed_update_page() {
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(&base, "test_page_txn_failed_update_page"))
        };

        let version = new_version(1 << 10);
        let page_table = PageTable::default();
        let guard = Guard::new(version, &page_table, &files);

        // insert old page.
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).unwrap();
        assert!(page_txn.update_page(id, addr, new).is_ok());

        // operate is failed.
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        assert!(page_txn.update_page(id, 1, addr).is_err());
    }

    #[test]
    fn page_txn_increment_page_addr_update() {
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(
                &base,
                "test_page_increment_page_addr_update",
            ))
        };

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, &page_table, &files);
        let page_txn = guard.begin();
        assert!(matches!(page_txn.update_page(1, 3, 2), Err(None)));
    }

    #[test]
    fn page_txn_replace_page() {
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(&base, "test_page_txn_replace_page"))
        };

        let version = new_version(1 << 10);
        let page_table = PageTable::default();
        let guard = Guard::new(version, &page_table, &files);
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).unwrap();
        assert!(page_txn.replace_page(id, addr, new, &[1, 2, 3]).is_ok());
    }

    #[test]
    fn page_txn_seal_write_buffer() {
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(&base, "test_page_seal_write_buffer"))
        };

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, &page_table, &files);
        let mut page_txn = guard.begin();
        page_txn.seal_write_buffer();
    }

    #[test]
    fn page_txn_insert_page() {
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(&base, "test_page_insert_page"))
        };

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, &page_table, &files);
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        page_txn.commit();

        assert_eq!(page_table.get(id), addr);
    }
}
