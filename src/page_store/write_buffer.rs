use std::{
    ptr::NonNull,
    sync::atomic::{AtomicU64, Ordering},
};

use bitflags::bitflags;

use super::Result;
use crate::{
    page::{PageBuf, PagePtr, PageRef},
    page_store::Error,
};

pub(crate) struct WriteBuffer {
    file_id: u32,

    buf: NonNull<u8>,
    buf_size: usize,

    // The state of current buffer, see [`BufferState`] for details.
    buffer_state: AtomicU64,
}

#[derive(Default, Debug, Clone)]
struct BufferState {
    sealed: bool,

    /// The number of txn in progress.
    num_writer: u32,

    /// The size of the allocated buffers for a [`WriteBuffer`], aligned by 8
    /// bytes.
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

/// [`ReleaseState`] indicates that caller whether to notify flush job.
#[derive(Debug)]
pub(crate) enum ReleaseState {
    /// The [`WriteBuffer`] might be active or still exists pending writer.
    None,
    /// The [`WriteBuffer`] has been sealed and all writers are released.
    Flush,
}

impl WriteBuffer {
    pub fn with_capacity(file_id: u32, size: u32) -> Self {
        use std::alloc::{alloc, Layout};

        let buf_size = size as usize;
        if buf_size <= core::mem::size_of::<usize>() {
            panic!("The capacity of WriteBuffer is too small");
        }

        if !buf_size.is_power_of_two() {
            panic!("The capacity of WriteBuffer is not pow of two");
        }

        let layout = Layout::from_size_align(buf_size, core::mem::size_of::<usize>())
            .expect("Invalid layout");
        let buf = unsafe {
            // Safety: it is guaranteed that layout has non-zero size.
            NonNull::new(alloc(layout)).expect("The memory is exhausted")
        };
        let default_state = BufferState::default();
        WriteBuffer {
            file_id,
            buf,
            buf_size,
            buffer_state: AtomicU64::new(default_state.apply()),
        }
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
    /// Before the writer is released, it must be ensured that all former
    /// allocated [`PageBuf`] have been released or converted to [`PageRef`]
    /// to avoid violating pointer aliasing rules.
    pub unsafe fn release_writer(&self) -> ReleaseState {
        todo!()
    }

    /// Seal the [`WriteBuffer`]. `Err(Error::Again)` is returned if the buffer
    /// has been sealed.
    ///
    /// # Safety
    ///
    /// Before the writer is released if `release_writer` is set, it must be
    /// ensured that all former allocated [`PageBuf`] have been released or
    /// converted to [`PageRef`] to avoid violating pointer aliasing rules.
    pub unsafe fn seal(&self, release_writer: bool) -> Result<ReleaseState> {
        let mut current = self.buffer_state.load(Ordering::Acquire);
        loop {
            let mut buffer_state = BufferState::load(current);
            if buffer_state.sealed {
                return Err(Error::Again);
            }

            buffer_state.set_sealed();
            if release_writer {
                buffer_state.dec_writer();
            }
            let new = buffer_state.apply();

            match self.buffer_state.compare_exchange(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if buffer_state.has_writer() {
                        return Ok(ReleaseState::None);
                    } else {
                        return Ok(ReleaseState::Flush);
                    }
                }
                Err(v) => {
                    current = v;
                }
            }
        }
    }

    /// Return an iterator to iterate records in the buffer.
    ///
    /// # Panic
    ///
    /// This function will panic if the the [`WriteBuffer`] is not flushable, to
    /// ensure that pointer aliasing rules are not violated.
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
    /// Users need to ensure that the accessed page has no mutable references,
    /// so as not to violate the rules of pointer aliasing.
    pub unsafe fn page(&self, page_addr: u64) -> PageRef {
        todo!()
    }
}

impl Drop for WriteBuffer {
    fn drop(&mut self) {
        use std::alloc::{dealloc, Layout};

        let state = BufferState::load(self.buffer_state.load(Ordering::SeqCst));
        if state.has_writer() {
            panic!("Try drop a write buffer that is still in use");
        }

        let layout = Layout::from_size_align(self.buf_size, core::mem::size_of::<usize>())
            .expect("Invalid layout");
        unsafe {
            // Safety: this memory is allocated in [`WriteBuffer::with_capacity`] and has
            // the same layout.
            dealloc(self.buf.as_ptr(), layout);
        }
    }
}

/// # Safety
///
/// [`WriteBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Send for WriteBuffer {}

/// # Safety
///
/// [`WriteBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Sync for WriteBuffer {}

impl BufferState {
    fn load(mut val: u64) -> Self {
        let allocated = (val & ((1 << 32) - 1)) as u32;
        let num_writer = ((val >> 32) & ((1 << 31) - 1)) as u32;
        let sealed = val & (1 << 63) != 0;
        BufferState {
            sealed,
            num_writer,
            allocated,
        }
    }

    #[inline]
    pub fn has_writer(&self) -> bool {
        self.num_writer > 0
    }

    #[inline]
    pub fn set_sealed(&mut self) {
        self.sealed = true;
    }

    #[inline]
    pub fn inc_writer(&mut self) {
        self.num_writer = self
            .num_writer
            .checked_add(1)
            .expect("inc writer out of range");
    }

    #[inline]
    pub fn dec_writer(&mut self) {
        self.num_writer = self
            .num_writer
            .checked_sub(1)
            .expect("dec writer out of range");
    }

    #[inline]
    pub fn alloc_size(&mut self, required: u32, buf_size: u32) -> Result<u32> {
        const ALIGN: u32 = core::mem::size_of::<usize>() as u32;
        debug_assert_eq!(self.allocated % ALIGN, 0);
        let required = required + (ALIGN - required % ALIGN);
        if self.allocated + required > buf_size {
            todo!("out of range")
        }

        let offset = self.allocated;
        self.allocated = offset + required;
        Ok(offset)
    }

    fn apply(&self) -> u64 {
        assert!(self.num_writer < (1 << 31));

        (if self.sealed { 1 << 63 } else { 0 })
            | ((self.num_writer as u64) << 32)
            | (self.allocated as u64)
    }
}

impl RecordHeader {
    pub unsafe fn from_raw<'a>(addr: u64) -> &'a mut Self {
        todo!()
    }

    /// Returns the total space of the current record, including the
    /// [`RecordHeader`].
    ///
    /// This value is not simply equal to `page_size +
    /// size_of::<RecordHeader>()`, because size of records need to be
    /// aligned by 8 bytes.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_store::Error;

    #[test]
    fn buffer_state_load_and_apply() {
        let mut state = BufferState::default();
        assert!(!state.sealed);
        assert_eq!(state.num_writer, 0);
        assert_eq!(state.allocated, 0);

        state.set_sealed();
        state.inc_writer();
        state.alloc_size(3, 1024).unwrap();
        let raw = state.apply();

        let state = BufferState::load(raw);
        assert!(state.sealed);
        assert_eq!(state.num_writer, 1);
        assert_eq!(state.allocated, 8);
    }

    #[test]
    fn write_buffer_construct_and_drop() {
        let buf = WriteBuffer::with_capacity(1, 512);
        drop(buf);
    }

    #[test]
    #[should_panic]
    fn write_buffer_capacity_is_power_of_two() {
        WriteBuffer::with_capacity(1, 513);
    }

    #[test]
    fn write_buffer_seal() {
        let buf = WriteBuffer::with_capacity(1, 512);
        assert!(matches!(
            unsafe { buf.seal(false) },
            Ok(ReleaseState::Flush)
        ));
    }

    #[test]
    fn write_buffer_sealed_seal() {
        let buf = WriteBuffer::with_capacity(1, 512);
        unsafe { buf.seal(false) }.unwrap();
        assert!(matches!(unsafe { buf.seal(false) }, Err(Error::Again)));
    }

    #[test]
    #[should_panic]
    fn write_buffer_empty_writer_release_seal() {
        let buf = WriteBuffer::with_capacity(1, 512);
        unsafe { buf.seal(true) };
    }
}
