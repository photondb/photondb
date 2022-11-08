use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::{
    version::Version,
    write_buffer::{RecordHeader, ReleaseState},
    Error, PageFiles, PageTable, Result, WriteBuffer, NAN_ID,
};
use crate::{
    env::Env,
    page::{PageBuf, PageRef},
};

pub(crate) struct Guard<'a, E: Env>
where
    Self: Send,
{
    version: Arc<Version>,
    page_table: &'a PageTable,
    page_files: &'a PageFiles<E>,
    owned_pages: Mutex<Vec<Vec<u8>>>,
}

impl<'a, E: Env> Guard<'a, E> {
    pub(crate) fn new(
        version: Arc<Version>,
        page_table: &'a PageTable,
        page_files: &'a PageFiles<E>,
    ) -> Self {
        Guard {
            version,
            page_table,
            page_files,
            owned_pages: Mutex::default(),
        }
    }

    pub(crate) fn begin(&self) -> PageTxn<E> {
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
            hold_write_guard: false,
            records: HashMap::default(),
            page_ids: Vec::default(),
        }
    }

    /// Returns the address of the corresponding page.
    ///
    /// Returns 0 if the page is not found.
    ///
    /// # Panics
    ///
    /// Panics if the page id is larger than [`MAX_ID`].
    ///
    /// [`MAX_ID`]: super::MAX_ID
    #[inline]
    pub(crate) fn page_addr(&self, id: u64) -> u64 {
        self.page_table.get(id)
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
            assert_eq!(file_info.get_file_id(), file_id);
            let Some(handle) = file_info.get_page_handle(addr) else {
                panic!("The addr {addr} is not belongs to the target page file {file_id}");
            };

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
pub(crate) struct PageTxn<'a, E: Env>
where
    Self: Send,
{
    guard: &'a Guard<'a, E>,

    file_id: u32,
    hold_write_guard: bool,
    records: HashMap<u64 /* page addr */, &'a mut RecordHeader>,
    page_ids: Vec<u64>,
}

impl<'a, E: Env> PageTxn<'a, E> {
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
    ///
    /// # Panics
    ///
    /// Panics if `addr` is not allocated by this transaction.
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
    ///
    /// # Panics
    ///
    /// Panics if `new_addr` is not allocated by this transaction.
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
    ///
    /// # Panics
    ///
    /// Panics if `new_addr` is not allocated by this transaction.
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

    /// Deallocates some pages.
    ///
    /// The `former_file_id` indicates that the corresponding file could be
    /// deleted if the dealloc pages record is persistent.
    pub(crate) fn dealloc_pages(
        mut self,
        former_file_id: Option<u32>,
        dealloc_addrs: &[u64],
    ) -> Result<()> {
        let header = self.dealloc_pages_impl(dealloc_addrs)?;
        if let Some(file_id) = former_file_id {
            header.set_former_file_id(file_id);
        }
        self.commit();
        Ok(())
    }

    fn seal_write_buffer(&mut self) {
        let release_state = {
            let buffer_set = self.guard.version.buffer_set.current();
            let write_buffer = buffer_set
                .write_buffer(self.file_id)
                .expect("The memory buffer should exists");

            match write_buffer.seal() {
                Ok(release_state) => release_state,
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
    fn alloc_page_impl(
        &mut self,
        page_size: u32,
    ) -> Result<(u64, &'a mut RecordHeader, PageBuf<'a>)> {
        self.with_write_guard(|buf, is_first_op| unsafe {
            // Safety: [`guard`] guarantees the lifetime of the page reference.
            buf.alloc_page(NAN_ID, page_size, is_first_op)
        })
    }

    #[inline]
    fn dealloc_pages_impl(&mut self, page_addrs: &[u64]) -> Result<&'a mut RecordHeader> {
        self.with_write_guard(|buf, is_first_op| unsafe {
            // Safety: [`guard`] guarantees the lifetime of the page reference.
            buf.dealloc_pages(page_addrs, is_first_op)
        })
    }

    #[inline]
    fn with_write_guard<F, O>(&mut self, f: F) -> Result<O>
    where
        F: FnOnce(&WriteBuffer, bool) -> Result<O>,
    {
        let is_first_op = !self.hold_write_guard;
        self.guard
            .version
            .with_write_buffer(self.file_id, |buf| f(buf, is_first_op))
            .map(|val| {
                self.hold_write_guard = true;
                val
            })
            .map_err(|err| {
                assert!(matches!(err, Error::Again));
                self.seal_write_buffer();
                err
            })
    }

    /// Commits the transaction.
    pub(crate) fn commit(mut self) {
        self.page_ids.clear();
        if self.hold_write_guard {
            self.records.clear();
            self.drop_writer_guard();
            self.hold_write_guard = false;
        }
    }

    #[inline]
    fn drop_writer_guard(&mut self) {
        assert!(self.hold_write_guard);
        let release_state = self
            .guard
            .version
            .with_write_buffer(self.file_id, |buf| unsafe {
                // Safety: all mutable references are released.
                buf.release_writer()
            });
        if matches!(release_state, ReleaseState::Flush) {
            self.guard.version.buffer_set.notify_flush_job();
        }
    }
}

impl<'a, E: Env> Drop for PageTxn<'a, E> {
    fn drop(&mut self) {
        for id in &self.page_ids {
            // TODO: safety conditions.
            unsafe { self.guard.page_table.dealloc(*id) };
        }

        if self.hold_write_guard {
            for header in self.records.values_mut() {
                header.set_tombstone();
            }
            self.records.clear();
            self.drop_writer_guard();
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

    #[photonio::test]
    async fn page_txn_update_page() {
        let env = crate::env::Photon;
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(env, &base, "test_page_txn_update_page").await)
        };

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), &page_table, &files);
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).unwrap();
        assert!(page_txn.update_page(id, addr, new).is_ok());

        assert_current_buffer_is_flushable(version);
    }

    #[photonio::test]
    async fn page_txn_failed_update_page() {
        let env = crate::env::Photon;
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(env, &base, "test_page_txn_failed_update_page").await)
        };

        let version = new_version(1 << 10);
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), &page_table, &files);

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

        assert_current_buffer_is_flushable(version);
    }

    #[photonio::test]
    async fn page_txn_increment_page_addr_update() {
        let env = crate::env::Photon;
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(env, &base, "test_page_increment_page_addr_update").await)
        };

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, &page_table, &files);
        let page_txn = guard.begin();
        assert!(matches!(page_txn.update_page(1, 3, 2), Err(None)));
    }

    #[photonio::test]
    async fn page_txn_replace_page() {
        let env = crate::env::Photon;
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(env, &base, "test_page_txn_replace_page").await)
        };

        let version = new_version(1 << 10);
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), &page_table, &files);
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).unwrap();
        assert!(page_txn.replace_page(id, addr, new, &[1, 2, 3]).is_ok());

        assert_current_buffer_is_flushable(version);
    }

    #[photonio::test]
    async fn page_txn_seal_write_buffer() {
        let env = crate::env::Photon;
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(env, &base, "test_page_seal_write_buffer").await)
        };

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, &page_table, &files);
        let mut page_txn = guard.begin();
        page_txn.seal_write_buffer();
    }

    #[photonio::test]
    async fn page_txn_seal_write_buffer_twice() {
        let env = crate::env::Photon;
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(env, &base, "test_page_seal_write_buffer_twice").await)
        };

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, &page_table, &files);
        let mut page_txn_1 = guard.begin();
        let mut page_txn_2 = guard.begin();
        page_txn_1.seal_write_buffer();
        page_txn_2.seal_write_buffer();
    }

    #[photonio::test]
    async fn page_txn_insert_page() {
        env_logger::init();
        let env = crate::env::Photon;
        let files = {
            let base = std::env::temp_dir();
            Arc::new(PageFiles::new(env, &base, "test_page_insert_page").await)
        };

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), &page_table, &files);
        let mut page_txn = guard.begin();
        let (addr, _) = page_txn.alloc_page(123).unwrap();
        let id = page_txn.insert_page(addr);
        page_txn.commit();

        assert_eq!(page_table.get(id), addr);
        assert_current_buffer_is_flushable(version);
    }

    fn assert_current_buffer_is_flushable(version: Arc<Version>) {
        let current = version.buffer_set.current();
        let buf = current.last_writer_buffer();
        buf.seal().unwrap();
        assert!(buf.is_flushable());
    }
}
