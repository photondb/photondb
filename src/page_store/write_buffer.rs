use std::{
    mem::MaybeUninit,
    ptr::NonNull,
    sync::atomic::{AtomicU64, Ordering},
};

use bitflags::bitflags;

use super::Result;
use crate::{
    page::{PageBuf, PageRef},
    page_store::Error,
};

pub(crate) struct WriteBuffer
where
    Self: Send,
{
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
    DeallocPages(DeallocPagesRecordRef<'a>),
}

pub(crate) struct DeallocPagesRecordRef<'a> {
    dealloc_pages: &'a [u64],
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

    #[inline]
    pub(crate) fn is_flushable(&self) -> bool {
        self.buffer_state().is_flushable()
    }

    #[inline]
    pub(crate) fn is_sealed(&self) -> bool {
        self.buffer_state().sealed
    }

    /// Allocate pages and record dealloc pages in one batch. This operation
    /// will acquire a writer guard.
    #[cfg(test)]
    pub(crate) fn batch(
        &self,
        new_page_list: &[(u64 /* page id */, u32 /* page size */)],
        dealloc_pages: &[u64],
    ) -> Result<(
        Vec<(u64, &mut RecordHeader, PageBuf)>,
        Option<&mut RecordHeader>,
    )> {
        const ALIGN: u32 = core::mem::size_of::<usize>() as u32;
        let dealloc_pages_size = (dealloc_pages.len() * core::mem::size_of::<u64>()) as u32;
        let need = new_page_list
            .iter()
            .map(|(_, v)| record_size(*v))
            .sum::<u32>()
            + record_size(dealloc_pages_size);
        debug_assert_eq!(need % ALIGN, 0);

        let mut offset = self.alloc_size(need, true)?;
        let mut records = Vec::with_capacity(new_page_list.len());
        for (page_id, page_size) in new_page_list {
            let (page_id, page_size) = (*page_id, *page_size);
            // Safety: here is the only one reference to the record.
            let (page_addr, header, page_buf) =
                unsafe { self.new_page_at(offset, page_id, page_size) };
            offset += header.record_size();
            records.push((page_addr, header, page_buf));
        }

        let dealloc_pages_header = if !dealloc_pages.is_empty() {
            // Safety: here is the only one reference to the record.
            let (header, body) =
                unsafe { self.new_dealloc_pages_record_at(offset, dealloc_pages.len()) };
            body.copy_from_slice(dealloc_pages);
            Some(header)
        } else {
            None
        };

        return Ok((records, dealloc_pages_header));
    }

    /// Allocate new page from the buffer.
    ///
    /// # Safety
    ///
    /// The user needs to ensure that the [`WriteBuffer`] is valid for as long
    /// as the page reference is still held.
    pub(crate) unsafe fn alloc_page<'a>(
        &self,
        page_id: u64,
        page_size: u32,
        acquire_writer: bool,
    ) -> Result<(u64, &'a mut RecordHeader, PageBuf<'a>)> {
        let acquire_size = record_size(page_size);
        let offset = self.alloc_size(acquire_size, acquire_writer)?;
        // Safety: here is the only one reference to the record.
        Ok(unsafe { self.new_page_at(offset, page_id, page_size) })
    }

    /// Deallocate pages.
    ///
    /// # Safety
    ///
    /// The user needs to ensure that the [`WriteBuffer`] is valid for as long
    /// as the page reference is still held.
    pub(crate) unsafe fn dealloc_pages<'a>(
        &self,
        page_addrs: &[u64],
        acquire_writer: bool,
    ) -> Result<&'a mut RecordHeader> {
        let dealloc_pages_size = (page_addrs.len() * core::mem::size_of::<u64>()) as u32;
        let acquire_size = record_size(dealloc_pages_size);
        let offset = self.alloc_size(acquire_size, acquire_writer)?;
        // Safety: here is the only one reference to the record.
        let (header, body) = unsafe { self.new_dealloc_pages_record_at(offset, page_addrs.len()) };
        body.copy_from_slice(page_addrs);
        Ok(header)
    }

    /// Release the writer guard acquired before.
    ///
    /// # Safety
    ///
    /// Before the writer is released, it must be ensured that all former
    /// allocated [`PageBuf`] have been released or converted to [`PageRef`]
    /// to avoid violating pointer aliasing rules.
    pub(crate) unsafe fn release_writer(&self) -> ReleaseState {
        let mut current = self.buffer_state.load(Ordering::Acquire);
        loop {
            let mut buffer_state = BufferState::load(current);
            buffer_state.dec_writer();
            let new = buffer_state.apply();

            match self.buffer_state.compare_exchange(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if buffer_state.is_flushable() {
                        return ReleaseState::Flush;
                    } else {
                        return ReleaseState::None;
                    }
                }
                Err(v) => {
                    current = v;
                }
            }
        }
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
                if release_writer {
                    return Ok(self.release_writer());
                }
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
    /// Panic if the `page_addr` is not aligned with
    /// `core::mem::size_of::<usize>()`.
    /// Panic if the `page_addr` is not a valid page.
    ///
    /// # Safety
    ///
    /// Users need to ensure that the accessed page has no mutable references,
    /// so as not to violate the rules of pointer aliasing.
    pub(crate) unsafe fn page<'a>(&self, page_addr: u64) -> PageRef<'a> {
        const ALIGN: u32 = core::mem::size_of::<usize>() as u32;

        let file_id = (page_addr >> 32) as u32;
        let offset = (page_addr & ((1 << 32) - 1)) as u32;

        if file_id != self.file_id {
            panic!("The specified addr is not belongs to the buffer");
        }

        if offset % ALIGN != 0 {
            panic!("The specified addr is not satisfied the align requirement");
        }

        let offset = offset
            .checked_sub(core::mem::size_of::<RecordHeader>() as u32)
            .expect("The specified addr is not a valid page");

        let header = self.record(offset);
        if let Some(RecordRef::Page(page_ref)) = header.record_ref() {
            return page_ref;
        }

        panic!("The specified addr is not a valid page");
    }

    /// Construct the reference of [`RecordHeader`] of the corresponding offset.
    ///
    /// # Panic
    ///
    /// See [`WriteBuffer::record_uninit`].
    ///
    /// # Safety
    ///
    /// Caller should ensure the specified offset of record has been
    /// initialized.
    #[inline]
    unsafe fn record(&self, offset: u32) -> &RecordHeader {
        self.record_uninit(offset).assume_init_ref()
    }

    /// Construct the reference of [`RecordHeader`] of the corresponding offset.
    /// The record might uninitialized.
    ///
    /// # Panic
    ///
    /// Panic if the offset is not aligned with `core::mem::size_of::<usize>()`.
    /// Panic if the offset exceeds the size of buffer.
    #[inline]
    fn record_uninit(&self, offset: u32) -> &MaybeUninit<RecordHeader> {
        let offset = offset as usize;
        if offset % core::mem::size_of::<usize>() != 0 {
            panic!("The specified offset is not aligned with pointer size");
        }

        assert!(offset + core::mem::size_of::<RecordHeader>() < self.buf_size);

        unsafe {
            // Safety:
            // 1. Both start and result pointer in bounds.
            // 2. The computed offset is not exceeded `isize`.
            &*(self
                .buf
                .as_ptr()
                .offset(offset as isize)
                .cast::<MaybeUninit<RecordHeader>>())
        }
    }

    /// Construct the mutable reference of [`RecordHeader`] of the corresponding
    /// offset. The record might uninitialized.
    ///
    /// # Safety
    ///
    /// There should no any references pointer to the target record.
    #[inline]
    unsafe fn record_uninit_mut<'a>(&self, offset: u32) -> &'a mut MaybeUninit<RecordHeader> {
        &mut *(self.record_uninit(offset) as *const _ as *mut _)
    }

    #[inline]
    fn buffer_state(&self) -> BufferState {
        BufferState::load(self.buffer_state.load(Ordering::Acquire))
    }

    /// Allocate memory and install writer. Returns the address of the first
    /// byte.
    fn alloc_size(&self, need: u32, acquire_writer: bool) -> Result<u32> {
        let mut current = self.buffer_state.load(Ordering::Acquire);
        loop {
            let mut state = BufferState::load(current);
            if state.sealed {
                return Err(Error::Again);
            }

            if acquire_writer {
                state.inc_writer();
            }
            let offset = state.alloc_size(need, self.buf_size as u32)?;
            let new = state.apply();
            match self.buffer_state.compare_exchange(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(offset);
                }
                Err(e) => {
                    current = e;
                }
            }
        }
    }

    /// New page at the corresponding offset.
    ///
    /// # Safety
    ///
    /// Not reference pointer to the target record.
    unsafe fn new_page_at<'a>(
        &self,
        offset: u32,
        page_id: u64,
        page_size: u32,
    ) -> (u64, &'a mut RecordHeader, PageBuf<'a>) {
        // Construct `RecordHeader`.
        // Safety: here is the only one reference to the record.
        let header = unsafe { self.record_uninit_mut(offset) };
        header.write(RecordHeader {
            page_id,
            flags: RecordFlags::NORMAL_PAGE.bits(),
            page_size,
        });
        let header = unsafe { header.assume_init_mut() };

        // Compute page addr.
        let page_offset = offset + core::mem::size_of::<RecordHeader>() as u32;
        let page_addr = ((self.file_id as u64) << 32) | (page_offset as u64);

        // Construct `PageBuf`.
        let buf = unsafe {
            let ptr = (header as *mut RecordHeader).offset(1).cast::<u8>();
            std::slice::from_raw_parts_mut(ptr, page_size as usize)
        };
        let page_buf = PageBuf::new(buf);

        (page_addr, header, page_buf)
    }

    /// New dealloc pages record at the corresponding offset.
    ///
    /// # Safety
    ///
    /// Not reference pointer to the target record.
    unsafe fn new_dealloc_pages_record_at<'a>(
        &self,
        offset: u32,
        num_dealloc_pages: usize,
    ) -> (&'a mut RecordHeader, &'a mut [u64]) {
        let page_size = (num_dealloc_pages * core::mem::size_of::<u64>()) as u32;

        // Safety: here is the only one reference to the record.
        let header = unsafe { self.record_uninit_mut(offset) };
        header.write(RecordHeader {
            page_id: 0,
            flags: RecordFlags::DELETED_PAGES.bits(),
            page_size,
        });
        let header = unsafe { header.assume_init_mut() };

        let body = unsafe {
            let ptr = (header as *mut RecordHeader).offset(1).cast::<u64>();
            std::slice::from_raw_parts_mut(ptr, num_dealloc_pages)
        };

        (header, body)
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
    #[inline]
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
    fn is_flushable(&self) -> bool {
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
        let required = next_multiple_of_u32(required, ALIGN);
        if self.allocated + required > buf_size {
            return Err(Error::Again);
        }

        let offset = self.allocated;
        self.allocated = offset + required;
        Ok(offset)
    }

    #[inline]
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
    #[inline]
    fn record_size(&self) -> u32 {
        record_size(self.page_size)
    }

    #[inline]
    pub(crate) fn is_tombstone(&self) -> bool {
        self.flags == RecordFlags::TOMBSTONE.bits()
    }

    #[inline]
    pub(crate) fn set_tombstone(&mut self) {
        self.flags = RecordFlags::TOMBSTONE.bits();
    }

    #[inline]
    pub(crate) fn page_id(&self) -> u64 {
        self.page_id
    }

    #[inline]
    pub(crate) fn set_page_id(&mut self, page_id: u64) {
        self.page_id = page_id;
    }

    fn record_ref<'a>(&self) -> Option<RecordRef<'a>> {
        match RecordFlags::from_bits_truncate(self.flags) {
            RecordFlags::NORMAL_PAGE => {
                let buf = unsafe {
                    // Safety: the target pointer is valid and initialized.
                    let ptr = (self as *const RecordHeader).offset(1).cast::<u8>();
                    std::slice::from_raw_parts(ptr, self.page_size as usize)
                };
                Some(RecordRef::Page(PageRef::new(buf)))
            }
            RecordFlags::DELETED_PAGES => {
                let size = self.page_size as usize / core::mem::size_of::<u64>();
                assert_eq!(size * core::mem::size_of::<u64>(), self.page_size as usize);
                let record = unsafe {
                    // Safety: the target address is valid and initialized.
                    let addr = (self as *const RecordHeader).offset(1).cast::<u64>();
                    std::slice::from_raw_parts(addr, size)
                };
                let val = DeallocPagesRecordRef {
                    dealloc_pages: record,
                    access_index: 0,
                };
                Some(RecordRef::DeallocPages(val))
            }
            _ => None,
        }
    }
}

impl<'a> Iterator for RecordIterator<'a> {
    type Item = (u64 /* page_addr */, &'a RecordHeader, RecordRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let buffer_state =
            BufferState::load(self.write_buffer.buffer_state.load(Ordering::Acquire));
        assert!(buffer_state.is_flushable());

        loop {
            if self.offset >= buffer_state.allocated {
                return None;
            }

            let record_offset = self.offset;
            // Safety: the request [`RecordHeader`] has been initialized (checked in above).
            let record_header = unsafe { self.write_buffer.record(record_offset) };

            self.offset += record_header.record_size();
            if let Some(record_ref) = record_header.record_ref() {
                let page_addr = ((self.write_buffer.file_id as u64) << 32) | (record_offset as u64);
                return Some((page_addr, record_header, record_ref));
            }
        }
    }
}

impl<'a> DeallocPagesRecordRef<'a> {
    pub(crate) fn as_slice(&self) -> &[u64] {
        self.dealloc_pages
    }
}

impl<'a> Iterator for DeallocPagesRecordRef<'a> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.access_index < self.dealloc_pages.len() {
            let item = self.dealloc_pages[self.access_index];
            self.access_index += 1;
            Some(item)
        } else {
            None
        }
    }
}

#[inline]
fn next_multiple_of_u32(val: u32, multiple: u32) -> u32 {
    ((val + multiple - 1) / multiple) * multiple
}

/// Returns the total space of the current record, including the
/// [`RecordHeader`].
///
/// This value is not simply equal to `page_size + size_of::<RecordHeader>()`,
/// because size of records need to be aligned by 8 bytes.
#[inline]
fn record_size(x: u32) -> u32 {
    const ALIGN: u32 = core::mem::size_of::<usize>() as u32;
    core::mem::size_of::<RecordHeader>() as u32 + next_multiple_of_u32(x, ALIGN)
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
    use std::collections::HashSet;

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
    fn record_header_record_size() {
        struct Test {
            page_size: u32,
            // Without `RecordHeader`.
            record_size: u32,
        }

        let tests = vec![
            Test {
                page_size: 1,
                record_size: 8,
            },
            Test {
                page_size: 8,
                record_size: 8,
            },
            Test {
                page_size: 15,
                record_size: 16,
            },
            Test {
                page_size: 16,
                record_size: 16,
            },
        ];
        for Test {
            page_size,
            record_size,
        } in tests
        {
            let header = RecordHeader {
                page_id: 0,
                flags: RecordFlags::NORMAL_PAGE.bits(),
                page_size,
            };
            assert_eq!(
                header.record_size(),
                core::mem::size_of::<RecordHeader>() as u32 + record_size
            );
        }
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
    fn write_buffer_sealed_but_write_inflights_seal() {
        // Even if the buffer is sealed, release writer is still needed.
        let buf = WriteBuffer::with_capacity(1, 1024);
        buf.batch(&[], &[1]).unwrap();
        unsafe { buf.seal(false) }.unwrap();
        assert!(matches!(unsafe { buf.seal(true) }, Ok(ReleaseState::Flush)));
    }

    #[test]
    #[should_panic]
    fn write_buffer_empty_writer_release_seal() {
        let buf = WriteBuffer::with_capacity(1, 512);
        unsafe { buf.seal(true).unwrap() };
    }

    #[test]
    fn write_buffer_iterate() {
        let buf = WriteBuffer::with_capacity(1, 1024);

        // 1. add pages
        buf.batch(
            &[(1, 20), (3, 40), (5, 60), (7, 80), (9, 100)],
            &[11, 12, 13, 14, 15],
        )
        .unwrap();
        unsafe { buf.release_writer() };

        // 2. add tombstones
        let (records_header, dealloc_pages_header) = buf.batch(&[(16, 17)], &[1, 2]).unwrap();
        records_header
            .into_iter()
            .for_each(|(_, h, _)| h.set_tombstone());
        dealloc_pages_header.map(|h| h.set_tombstone());

        unsafe { buf.seal(true) }.unwrap();

        let expect_dealloc_pages = vec![11, 12, 13, 14, 15];
        let mut active_pages: HashSet<u64> = vec![1, 3, 5, 7, 9].into_iter().collect();
        for (_, header, record_ref) in buf.iter() {
            match record_ref {
                RecordRef::Page(_page) => {
                    let page_id = header.page_id();
                    assert!(active_pages.remove(&page_id));
                }
                RecordRef::DeallocPages(dealloc_pages) => {
                    let dealloc_pages: Vec<u64> = dealloc_pages.collect();
                    assert_eq!(dealloc_pages, expect_dealloc_pages);
                }
            }
        }
        assert!(active_pages.is_empty());
    }

    #[test]
    fn write_buffer_pages_alloc() {
        let buf = WriteBuffer::with_capacity(1, 1 << 20);

        // 1. alloc normal pages
        unsafe { buf.alloc_page(1, 123, true) }.unwrap();

        // 2. dealloc pages
        unsafe { buf.dealloc_pages(&[5, 6, 7], false) }.unwrap();

        // 3. alloc but set page as tombstone.
        let (_, header, _) = unsafe { buf.alloc_page(2, 222, false) }.unwrap();
        header.set_tombstone();
        drop(header);

        let header = unsafe { buf.dealloc_pages(&[1, 3, 4], false) }.unwrap();
        header.set_tombstone();
        drop(header);

        unsafe { buf.release_writer() };
    }

    #[test]
    fn write_buffer_alloc_out_of_range() {
        let buf = WriteBuffer::with_capacity(1, 1 << 10);

        assert!(matches!(
            unsafe { buf.alloc_page(1, 2 << 10, true) },
            Err(Error::Again)
        ));
    }
}
