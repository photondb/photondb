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

pub(crate) struct DeletedPagesRecordRef<'a> {
    deleted_pages: &'a [u64],
    access_index: usize,
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
    pub(crate) fn with_capacity(file_id: u32, size: u32) -> Self {
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
    pub(crate) fn file_id(&self) -> u32 {
        self.file_id
    }

    pub(crate) fn is_flushable(&self) -> bool {
        todo!()
    }

    pub(crate) fn is_sealed(&self) -> bool {
        todo!()
    }

    /// Allocate new page from the buffer.
    pub(crate) fn alloc_page(
        &self,
        page_size: u32,
        acquire_writer: bool,
    ) -> Result<(u64, &mut RecordHeader, PageBuf)> {
        todo!()
    }

    pub(crate) fn save_deleted_pages(
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
    pub(crate) unsafe fn release_writer(&self) -> ReleaseState {
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
    pub(crate) unsafe fn seal(&self, release_writer: bool) -> Result<ReleaseState> {
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
    pub(crate) fn iter(&self) -> RecordIterator {
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
    pub(crate) unsafe fn page(&self, page_addr: u64) -> PageRef {
        todo!()
    }

    /// Construct the reference of [`RecordHeader`] of the corresponding offset.
    ///
    /// # Panic
    ///
    /// Panic if the offset is not aligned with `core::mem::size_of::<usize>()`.
    /// Panic if the offset exceeds the size of buffer.
    ///
    /// # Safety
    ///
    /// Caller should ensure the specified offset of record has been
    /// initialized.
    unsafe fn record(&self, offset: u32) -> &RecordHeader {
        let offset = offset as usize;
        if core::mem::size_of::<usize>() % offset != 0 {
            panic!("The specified offset is not aligned with pointer size");
        }

        assert!(offset + core::mem::size_of::<RecordHeader>() < self.buf_size);

        // Safety:
        // 1. Both start and result pointer in bounds.
        // 2. The computed offset is not exceeded `isize`.
        &*(self
            .buf
            .as_ptr()
            .offset(offset as isize)
            .cast::<RecordHeader>())
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
    fn load(val: u64) -> Self {
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
    fn has_writer(&self) -> bool {
        self.num_writer > 0
    }

    #[inline]
    fn flushable(&self) -> bool {
        self.sealed && !self.has_writer()
    }

    #[inline]
    fn set_sealed(&mut self) {
        self.sealed = true;
    }

    #[inline]
    fn inc_writer(&mut self) {
        self.num_writer = self
            .num_writer
            .checked_add(1)
            .expect("inc writer out of range");
    }

    #[inline]
    fn dec_writer(&mut self) {
        self.num_writer = self
            .num_writer
            .checked_sub(1)
            .expect("dec writer out of range");
    }

    #[inline]
    fn alloc_size(&mut self, required: u32, buf_size: u32) -> Result<u32> {
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
    /// Returns the total space of the current record, including the
    /// [`RecordHeader`].
    ///
    /// This value is not simply equal to `page_size +
    /// size_of::<RecordHeader>()`, because size of records need to be
    /// aligned by 8 bytes.
    fn record_size(&self) -> u32 {
        const ALIGN: u32 = core::mem::size_of::<usize>() as u32;
        ((self.page_size + ALIGN - 1) / ALIGN) * ALIGN
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        todo!()
    }

    pub(crate) fn set_tombstone(&mut self) {
        todo!()
    }

    pub(crate) fn page_ptr(&self) -> PagePtr {
        todo!()
    }

    #[inline]
    pub(crate) fn page_size(&self) -> u32 {
        self.page_size
    }

    #[inline]
    pub(crate) fn page_id(&self) -> u64 {
        self.page_id
    }

    fn record_ref(&self) -> Option<RecordRef> {
        match RecordFlags::from_bits_truncate(self.flags) {
            RecordFlags::NORMAL_PAGE => {
                let addr = unsafe {
                    // Safety: the target address is valid and initialized.
                    let addr = (self as *const RecordHeader).offset(1).cast::<u8>();
                    NonNull::new_unchecked(addr as *mut u8)
                };
                let size = self.page_size;
                Some(RecordRef::Page(PageRef::new(PagePtr::new(
                    addr,
                    size as usize,
                ))))
            }
            RecordFlags::DELETED_PAGES => {
                let size = self.page_size as usize / core::mem::size_of::<u64>();
                assert_eq!(size * core::mem::size_of::<u64>(), self.page_size as usize);
                let record = unsafe {
                    // Safety: the target address is valid and initialized.
                    let addr = (self as *const RecordHeader).offset(1).cast::<u64>();
                    std::slice::from_raw_parts(addr, size)
                };
                let val = DeletedPagesRecordRef {
                    deleted_pages: record,
                    access_index: 0,
                };
                Some(RecordRef::DeletedPages(val))
            }
            _ => None,
        }
    }
}

impl<'a> Iterator for RecordIterator<'a> {
    type Item = (&'a RecordHeader, RecordRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let buffer_state =
            BufferState::load(self.write_buffer.buffer_state.load(Ordering::Acquire));
        assert!(buffer_state.flushable());

        loop {
            if self.offset >= buffer_state.allocated {
                return None;
            }

            // Safety: the request [`RecordHeader`] has been initialized (checked in above).
            let record_header = unsafe { self.write_buffer.record(self.offset) };

            self.offset += record_header.record_size();
            if record_header.is_tombstone() {
                continue;
            }

            if let Some(record_ref) = record_header.record_ref() {
                return Some((record_header, record_ref));
            }
        }
    }
}

impl<'a> Iterator for DeletedPagesRecordRef<'a> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.access_index < self.deleted_pages.len() {
            let item = self.deleted_pages[self.access_index];
            self.access_index += 1;
            Some(item)
        } else {
            None
        }
    }
}

bitflags! {
    struct RecordFlags: u32 {
        const EMPTY         = 0b0000_0000;
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
        unsafe { buf.seal(true).unwrap() };
    }

    #[test]
    #[ignore]
    fn write_buffer_iterate() {
        let buf = WriteBuffer::with_capacity(1, 1024);

        // TODO: add some pages.

        unsafe { buf.seal(false) }.unwrap();
        for (header, record_ref) in buf.iter() {
            match record_ref {
                RecordRef::Page(_page) => {
                    let _ = header.page_id();
                }
                RecordRef::DeletedPages(_deleted_pages) => {}
            }
        }
    }
}
