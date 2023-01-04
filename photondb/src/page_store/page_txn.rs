use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bitflags::bitflags;

use super::{
    cache::CacheToken,
    stats::AtomicWritebufStats,
    version::Version,
    write_buffer::{RecordHeader, ReleaseState},
    CacheEntry, Error, LRUCache, PageFiles, PageTable, Result, WriteBuffer, NAN_ID,
};
use crate::{
    env::Env,
    page::{PageBuf, PageInfo, PageRef},
};

bitflags! {
/// Cache Option.
pub struct CacheOption: u32 {
    /// Default: read from cache first, read disk and refill cache as recent used when cache miss.
    const DEFAULT = 0b00000000;
    /// RefillColdWhenNotFull: read from cache first and read disk when cache miss.
    /// It will refill cache as cold when cache has space after cache miss and Not refill cache when cache already full.
    /// It's normally be used when read some cold data(not in cache) and discard them soon(i.g. consolidate)
    const REFILL_COLD_WHEN_NOT_FULL = 0b00000001;

    const HIGH_PRI = 0b00000010;

    const LOW_PRI = 0b00000100;
}
}

impl Default for CacheOption {
    fn default() -> Self {
        CacheOption::DEFAULT
    }
}

impl CacheOption {
    pub(crate) fn priority(&self) -> CachePriority {
        if self.contains(CacheOption::HIGH_PRI) {
            CachePriority::High
        } else if self.contains(CacheOption::LOW_PRI) {
            CachePriority::Low
        } else {
            CachePriority::Bottom
        }
    }

    pub(crate) fn set_priority(mut self, pri: CachePriority) -> Self {
        match pri {
            CachePriority::High => {
                self.set(CacheOption::HIGH_PRI, true);
                self.set(CacheOption::LOW_PRI, false);
            }
            CachePriority::Low => {
                self.set(CacheOption::HIGH_PRI, false);
                self.set(CacheOption::LOW_PRI, true);
            }
            CachePriority::Bottom => {
                self.set(CacheOption::HIGH_PRI, false);
                self.set(CacheOption::LOW_PRI, false);
            }
        };
        self
    }

    pub(crate) fn refill_cold_when_not_full(&self) -> bool {
        self.contains(CacheOption::REFILL_COLD_WHEN_NOT_FULL)
    }

    pub(crate) fn set_refill_cold_when_not_full(mut self, v: bool) -> Self {
        self.set(CacheOption::REFILL_COLD_WHEN_NOT_FULL, v);
        self
    }
}

pub enum CachePriority {
    High,
    Low,
    Bottom,
}

type CacheEntryGuard = CacheEntry<Vec<u8>, LRUCache<Vec<u8>>>;

pub(crate) struct Guard<E: Env>
where
    Self: Send,
{
    version: Arc<Version>,
    page_table: PageTable,
    page_files: Arc<PageFiles<E>>,
    cache_guards: Mutex<Vec<CacheEntryGuard>>,
    writebuf_stats: Arc<AtomicWritebufStats>,
}

impl<E: Env> Guard<E> {
    pub(crate) fn new(
        version: Arc<Version>,
        page_table: PageTable,
        page_files: Arc<PageFiles<E>>,
        writebuf_stats: Arc<AtomicWritebufStats>,
    ) -> Self {
        Guard {
            version,
            page_table,
            page_files,
            cache_guards: Mutex::default(),
            writebuf_stats,
        }
    }

    pub(crate) async fn begin(&self) -> PageTxn<E> {
        let buffer_id = self.version.buffer_set.acquire_active_buffer_id().await;
        PageTxn {
            guard: self,
            buffer_id,
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

    pub(crate) fn read_page_info(&self, addr: u64) -> Result<PageInfo> {
        let logical_id = (addr >> 32) as u32;
        if let Some(buf) = self.version.get(logical_id) {
            // Safety: all mutable references are released.
            let page = unsafe { buf.page(addr) };
            return Ok(page.info());
        }

        let Some(file_info) = self.version.page_groups().get(&logical_id) else {
            panic!("File {logical_id} (addr {addr}) is not exists");
        };

        let Some(page_info) = file_info.get_page_info(addr) else {
            panic!("The addr {addr} is not belongs to the target file");
        };

        Ok(page_info)
    }

    pub(crate) async fn read_page(
        &self,
        addr: u64,
        hint: CacheOption,
    ) -> Result<(PageRef, Option<CacheToken>)> {
        let logical_id = (addr >> 32) as u32;
        if let Some(buf) = self.version.get(logical_id) {
            self.writebuf_stats.read_in_buf.inc();
            // Safety: all mutable references are released.
            return Ok((unsafe { buf.page(addr) }, None));
        }
        self.writebuf_stats.read_in_file.inc();

        let Some(page_group) = self.version.page_groups().get(&logical_id) else {
            panic!("File {logical_id} (addr {addr}) is not exists");
        };

        let physical_id = page_group.meta().file_id;
        let Some(file_info) = self.version.file_infos().get(&physical_id) else {
            panic!("TODO:")
        };
        let Some(handle) = page_group.get_page_handle(addr) else {
            panic!("The addr {addr} is not belongs to the target file {physical_id:?}");
        };

        let (entry, hit) = self
            .page_files
            .read_page(physical_id, file_info.meta(), addr, handle, hint)
            .await?;

        let mut owned_pages = self.cache_guards.lock().expect("Poisoned");
        owned_pages.push(entry);

        let last_guard = owned_pages.last().unwrap();
        let page = last_guard.value();
        if !hit {
            self.writebuf_stats.read_file_bytes.add(page.len() as u64);
        }
        let cache_token = last_guard.cache_token();

        let page = PageRef::new(unsafe {
            // Safety: the lifetime is guarranted by `guard`.
            std::slice::from_raw_parts(page.as_ptr(), page.len())
        });

        if !hit && !page.tier().is_leaf() {
            self.writebuf_stats.miss_inner.inc();
        }

        Ok((page, Some(cache_token)))
    }
}

/// A transaction to manipulate pages in a page store.
///
/// A `PageTxn` may allocate memory from a `WriteBuffer`, allocate new entries
/// from `PageTable`, or update a `PageTable` entry with new page address.
/// All these operations are tracked by *records* and *page_ids*.
///
/// On commit, the transaction will clear these operations.
/// On drop, if there are operations exists, the transaction will be aborted and
/// all its operations will be rolled back.
pub(crate) struct PageTxn<'a, E: Env>
where
    Self: Send,
{
    guard: &'a Guard<E>,

    buffer_id: u32,
    // We may allocate multiple page buffers inside one PageTxn, for example when we split a tree
    // node. We only count the BufferState#num_writer once since we are dealing with the same
    // WriteBuffer. "hold_write_guard" is set to true when the first time we allocate a page
    // buffer from WriteBuffer.
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
    pub(crate) async fn alloc_page(&mut self, size: usize) -> Result<(u64, PageBuf<'a>)> {
        let page_size = size as u32;
        let (addr, header, buf) = self.alloc_page_impl(page_size).await?;
        self.records.insert(addr, header);
        Ok((addr, buf))
    }

    /// Inserts a new page into the store. Insertion happens when page splits or
    /// tree initializes. It returns the id of the inserted page.
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
    pub(crate) async fn replace_page(
        mut self,
        id: u64,
        old_addr: u64,
        new_addr: u64,
        dealloc_addrs: &[u64],
    ) -> Result<()> {
        if new_addr <= old_addr {
            return Err(Error::Again);
        }

        let dealloc_pages = self.dealloc_pages_impl(dealloc_addrs).await?;
        self.update_page(id, old_addr, new_addr).map_err(|_| {
            dealloc_pages.set_tombstone();
            Error::Again
        })?;
        Ok(())
    }

    #[inline]
    async fn alloc_page_impl(
        &mut self,
        page_size: u32,
    ) -> Result<(u64, &'a mut RecordHeader, PageBuf<'a>)> {
        self.with_write_guard(|buf, is_first_op| unsafe {
            // Safety: [`guard`] guarantees the lifetime of the page reference.
            buf.alloc_page(NAN_ID, page_size, is_first_op)
        })
        .await
    }

    #[inline]
    async fn dealloc_pages_impl(&mut self, page_addrs: &[u64]) -> Result<&'a mut RecordHeader> {
        self.with_write_guard(|buf, is_first_op| unsafe {
            // Safety: [`guard`] guarantees the lifetime of the page reference.
            buf.dealloc_pages(page_addrs, is_first_op)
        })
        .await
    }

    #[inline]
    async fn with_write_guard<F, O>(&mut self, f: F) -> Result<O>
    where
        F: FnOnce(&WriteBuffer, bool) -> Result<O>,
    {
        let is_first_op = !self.hold_write_guard;
        let result = {
            let buffer = self
                .guard
                .version
                .get(self.buffer_id)
                .expect("The target buffer must exists");
            f(&buffer, is_first_op)
        };
        match result {
            Ok(val) => {
                self.hold_write_guard = true;
                Ok(val)
            }
            Err(Error::TooLargeSize) => Err(Error::TooLargeSize),
            Err(Error::Again) => {
                self.guard
                    .version
                    .buffer_set
                    .switch_buffer(self.buffer_id)
                    .await;
                Err(Error::Again)
            }
            _ => unreachable!(),
        }
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
        let buf = self
            .guard
            .version
            .get(self.buffer_id)
            .expect("The target write buffer must exists");
        // Safety: all mutable references are released.
        let release_state = unsafe { buf.release_writer() };
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
    use super::*;
    use crate::{
        page_store::{
            page_table::PageTable,
            version::{DeltaVersion, Version},
        },
        PageStoreOptions,
    };

    fn new_version(size: u32) -> Arc<Version> {
        Arc::new(Version::new(size, 1, 8, DeltaVersion::default()))
    }

    #[photonio::test]
    async fn page_txn_update_page() {
        let env = crate::env::Photon;
        let base = tempdir::TempDir::new("test_page_txn_update_page").unwrap();
        let files = Arc::new(PageFiles::new(env, base.path(), &test_option()).await);
        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), page_table, files, Default::default());
        let mut page_txn = guard.begin().await;
        let (addr, _) = page_txn.alloc_page(123).await.unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).await.unwrap();
        assert!(page_txn.update_page(id, addr, new).is_ok());

        assert_current_buffer_is_flushable(version);
    }

    #[photonio::test]
    async fn page_txn_failed_update_page() {
        let env = crate::env::Photon;
        let base = tempdir::TempDir::new("test_page_txn_failed_update_page").unwrap();
        let files = Arc::new(PageFiles::new(env, base.path(), &test_option()).await);

        let version = new_version(1 << 10);
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), page_table, files, Default::default());

        // insert old page.
        let mut page_txn = guard.begin().await;
        let (addr, _) = page_txn.alloc_page(123).await.unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).await.unwrap();
        assert!(page_txn.update_page(id, addr, new).is_ok());

        // operate is failed.
        let mut page_txn = guard.begin().await;
        let (addr, _) = page_txn.alloc_page(123).await.unwrap();
        assert!(page_txn.update_page(id, 1, addr).is_err());

        assert_current_buffer_is_flushable(version);
    }

    #[photonio::test]
    async fn page_txn_increment_page_addr_update() {
        let env = crate::env::Photon;
        let base = tempdir::TempDir::new("test_page_increment_page_addr_update").unwrap();
        let files = Arc::new(PageFiles::new(env, base.path(), &test_option()).await);

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, page_table, files, Default::default());
        let page_txn = guard.begin().await;
        assert!(matches!(page_txn.update_page(1, 3, 2), Err(None)));
    }

    #[photonio::test]
    async fn page_txn_replace_page() {
        let env = crate::env::Photon;
        let base = tempdir::TempDir::new("test_page_txn_replace_page").unwrap();
        let files = Arc::new(PageFiles::new(env, base.path(), &test_option()).await);

        let version = new_version(1 << 10);
        let page_table = PageTable::default();
        let guard = Guard::new(version.clone(), page_table, files, Default::default());
        let mut page_txn = guard.begin().await;
        let (addr, _) = page_txn.alloc_page(123).await.unwrap();
        let id = page_txn.insert_page(addr);
        let (new, _) = page_txn.alloc_page(123).await.unwrap();
        assert!(page_txn
            .replace_page(id, addr, new, &[1, 2, 3])
            .await
            .is_ok());

        assert_current_buffer_is_flushable(version);
    }

    impl<'a, E: Env> PageTxn<'a, E> {
        async fn seal_write_buffer(&mut self) {
            self.guard
                .version
                .buffer_set
                .switch_buffer(self.buffer_id)
                .await;
        }
    }

    #[photonio::test]
    async fn page_txn_seal_write_buffer() {
        let env = crate::env::Photon;
        let base = tempdir::TempDir::new("test_page_seal_write_buffer").unwrap();
        let files = Arc::new(PageFiles::new(env, base.path(), &test_option()).await);

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, page_table, files, Default::default());
        let mut page_txn = guard.begin().await;
        page_txn.seal_write_buffer().await;
    }

    #[photonio::test]
    async fn page_txn_seal_write_buffer_twice() {
        let env = crate::env::Photon;
        let base = tempdir::TempDir::new("test_page_seal_write_buffer_twice").unwrap();
        let files = Arc::new(PageFiles::new(env, base.path(), &test_option()).await);

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(version, page_table, files, Default::default());
        let mut page_txn_1 = guard.begin().await;
        let mut page_txn_2 = guard.begin().await;
        page_txn_1.seal_write_buffer().await;
        page_txn_2.seal_write_buffer().await;
    }

    #[photonio::test]
    async fn page_txn_insert_page() {
        let env = crate::env::Photon;
        let base = tempdir::TempDir::new("test_page_insert_page").unwrap();
        let files = Arc::new(PageFiles::new(env, base.path(), &test_option()).await);

        let version = new_version(512);
        let page_table = PageTable::default();
        let guard = Guard::new(
            version.clone(),
            page_table.clone(),
            files,
            Default::default(),
        );
        let mut page_txn = guard.begin().await;
        let (addr, _) = page_txn.alloc_page(123).await.unwrap();
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

    fn test_option() -> PageStoreOptions {
        PageStoreOptions {
            cache_capacity: 2 << 10,
            ..Default::default()
        }
    }
}
