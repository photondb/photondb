use std::{ptr::NonNull, sync::atomic::AtomicU64};

use bitflags::bitflags;

use super::Result;
use crate::page::{PageBuf, PagePtr, PageRef};

pub(crate) struct WriteBuffer {
    file_id: u32,

    buf: NonNull<u8>,
    buf_size: usize,

    // The state of current buffer, see [`BufferState`] for details.
    buffer_state: AtomicU64,
}

struct BufferState {
    sealed: bool,

    /// The number of txn in progress.
    num_txn: u32,

    /// The size of the allocated buffers for a [`WriteBuffer`], aligned by 8 bytes.
    allocated: u32,
}

#[repr(C)]
pub(crate) struct RecordHeader {
    page_id: u64,
    flags: u32,
    page_size: u32,
}

pub(crate) struct RecordIterator<'a> {
    write_buffer: &'a WriteBuffer,
    offset: u32,
}

pub(crate) enum RecordRef<'a> {
    Page(PageRef<'a>),
    DeletedPages(DeletedPagesRecordRef<'a>),
}

#[repr(C)]
pub(crate) struct DeletePagesRecord {
    page_addrs: [u64],
}

pub(crate) struct DeletedPagesRecordRef<'a> {
    record: &'a mut DeletePagesRecord,
    access_index: usize,
    size: usize,
}

#[derive(Debug)]
pub(crate) enum ReleaseState {
    None,
    Flush,
}

impl WriteBuffer {
    pub fn with_capacity(size: u32) -> Self {
        todo!()
    }

    #[inline]
    pub fn file_id(&self) -> u32 {
        self.file_id
    }

    pub fn is_flushable(&self) -> bool {
        todo!()
    }

    pub fn is_sealed(&self) -> bool {
        todo!()
    }

    /// Allocate new page from the buffer.
    pub fn alloc_page(
        &self,
        page_size: u32,
        acquire_writer: bool,
    ) -> Result<(u64, &mut RecordHeader, PageBuf)> {
        todo!()
    }

    pub fn save_deleted_pages(
        &self,
        page_addrs: &[u64],
        acquire_writer: bool,
    ) -> Result<&mut RecordHeader> {
        todo!()
    }

    /// Release the writer guard acquired before.
    ///
    /// # Safety
    ///
    /// Before the writer is released, it must be ensured that all former allocated [`PageBuf`] have
    /// been released or converted to [`PageRef`] to avoid violating pointer aliasing rules.
    pub unsafe fn release_writer(&self) -> ReleaseState {
        todo!()
    }

    /// Seal the [`WriteBuffer`].
    ///
    /// # Safety
    ///
    /// Before the writer is released if `release_writer` is set, it must be ensured that all former
    /// allocated [`PageBuf`] have been released or converted to [`PageRef`] to avoid violating
    /// pointer aliasing rules.
    pub unsafe fn seal(&self, release_writer: bool) -> Result<()> {
        todo!()
    }

    /// Return an iterator to iterate records in the buffer.
    ///
    /// # Panic
    ///
    /// This function will panic if the the [`WriteBuffer`] is not flushable, to ensure that pointer
    /// aliasing rules are not violated.
    pub fn iter(&self) -> impl Iterator + '_ {
        RecordIterator {
            write_buffer: &self,
            offset: 0,
        }
    }

    /// Return the [`PageRef`] of the specified addr.
    ///
    /// # Panic
    ///
    /// Panic if the `page_addr` is not belongs to the [`WriteBuffer`].
    ///
    /// # Safety
    ///
    /// Users need to ensure that the accessed page has no mutable references, so as not to violate
    /// the rules of pointer aliasing.
    pub unsafe fn page(&self, page_addr: u64) -> PageRef {
        todo!()
    }
}

impl Drop for WriteBuffer {
    fn drop(&mut self) {
        todo!("check and release buf")
    }
}

/// # Safety
///
/// [`WriteBuffer`] is [`Send`] since all accesses to the inner buf are guaranteed that the aliases
/// do not overlap.
unsafe impl Send for WriteBuffer {}

/// # Safety
///
/// [`WriteBuffer`] is [`Send`] since all accesses to the inner buf are guaranteed that the aliases
/// do not overlap.
unsafe impl Sync for WriteBuffer {}

impl BufferState {
    fn load(val: u64) -> Self {
        todo!()
    }

    pub fn set_sealed(&mut self) {
        todo!()
    }

    pub fn inc_writer(&mut self) {
        todo!()
    }

    pub fn dec_writer(&mut self) {
        todo!()
    }

    pub fn alloc_size(&mut self, buf_size: u32) -> Result<u32> {
        todo!()
    }

    fn apply(&self) -> u64 {
        todo!()
    }
}

impl RecordHeader {
    pub unsafe fn from_raw<'a>(addr: u64) -> &'a mut Self {
        todo!()
    }

    /// Returns the total space of the current record, including the [`RecordHeader`].
    ///
    /// This value is not simply equal to `page_size + size_of::<RecordHeader>()`, because size of
    /// records need to be aligned by 8 bytes.
    pub fn record_size(&self) -> u32 {
        todo!()
    }

    pub fn is_tombstone(&self) -> bool {
        todo!()
    }

    pub fn set_tombstone(&mut self) {
        todo!()
    }

    pub fn page_ptr(&self) -> PagePtr {
        todo!()
    }

    #[inline]
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    #[inline]
    pub fn page_id(&self) -> u64 {
        self.page_id
    }
}

impl<'a> Iterator for RecordIterator<'a> {
    type Item = (&'a RecordHeader, RecordRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<'a> DeletedPagesRecordRef<'a> {
    pub fn from_raw(addr: u64) -> Self {
        todo!()
    }
}

impl<'a> Iterator for DeletedPagesRecordRef<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

bitflags! {
    struct RecordFlags: u32 {
        const NORMAL_PAGE   = 0b0000_0001;
        const DELETED_PAGES = 0b0000_0010;

        const TOMBSTONE     = 0b1000_0000;
    }
}
