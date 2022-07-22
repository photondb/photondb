use std::{
    alloc::{GlobalAlloc, Layout},
    marker::PhantomData,
};

// A page pointer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PagePtr {
    Mem(u64),
    Disk(u64),
}

const MEM_DISK_MASK: u64 = 1 << 63;

impl PagePtr {
    pub const fn null() -> Self {
        Self::Mem(0)
    }

    pub fn is_null(&self) -> bool {
        if let Self::Mem(0) = self {
            true
        } else {
            false
        }
    }
}

impl From<u64> for PagePtr {
    fn from(addr: u64) -> Self {
        if addr & MEM_DISK_MASK == 0 {
            Self::Mem(addr)
        } else {
            Self::Disk(addr & !MEM_DISK_MASK)
        }
    }
}

impl From<PagePtr> for u64 {
    fn from(ptr: PagePtr) -> u64 {
        match ptr {
            PagePtr::Mem(addr) => addr,
            PagePtr::Disk(addr) => addr | MEM_DISK_MASK,
        }
    }
}

// A mutable page buffer.
pub struct PageBuf {
    raw: RawPage,
    size: usize,
}

impl PageBuf {
    unsafe fn from_raw(ptr: *mut u8, size: usize) -> Self {
        debug_assert!(size >= PAGE_HEADER_SIZE);
        Self {
            raw: RawPage(ptr as u64),
            size,
        }
    }

    unsafe fn into_raw(self) -> *mut u8 {
        self.raw.0 as *mut u8
    }

    pub fn ver(&self) -> u64 {
        self.raw.ver()
    }

    pub fn set_ver(&mut self, ver: u64) {
        self.raw.set_ver(ver);
    }

    pub fn len(&self) -> u8 {
        self.raw.len()
    }

    pub fn set_len(&mut self, len: u8) {
        self.raw.set_len(len);
    }

    pub fn kind(&self) -> PageKind {
        self.raw.kind().into()
    }

    pub fn set_kind(&mut self, kind: PageKind) {
        self.raw.set_kind(kind.into());
    }

    pub fn next(&self) -> PagePtr {
        self.raw.next()
    }

    pub fn set_next(&mut self, next: PagePtr) {
        self.raw.set_next(next.into());
    }

    pub(super) fn content(&self) -> *const u8 {
        self.raw.content()
    }

    pub(super) fn content_mut(&mut self) -> *mut u8 {
        self.raw.content_mut()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn as_ptr(&self) -> PagePtr {
        PagePtr::Mem(self.raw.0)
    }

    pub fn as_ref(&self) -> PageRef {
        PageRef::new(self.raw.0)
    }
}

// An immutable page reference.
#[derive(Copy, Clone, Debug)]
pub struct PageRef<'a> {
    raw: RawPage,
    _mark: PhantomData<&'a ()>,
}

impl PageRef<'_> {
    fn new(ptr: u64) -> Self {
        Self {
            raw: RawPage(ptr),
            _mark: PhantomData,
        }
    }

    pub fn ver(&self) -> u64 {
        self.raw.ver()
    }

    pub fn len(&self) -> u8 {
        self.raw.len()
    }

    pub fn kind(&self) -> PageKind {
        self.raw.kind().into()
    }

    pub fn next(&self) -> PagePtr {
        self.raw.next()
    }

    pub(super) fn content(&self) -> *const u8 {
        self.raw.content()
    }
}

impl From<u64> for PageRef<'_> {
    fn from(ptr: u64) -> Self {
        Self::new(ptr)
    }
}

impl From<PageRef<'_>> for u64 {
    fn from(page: PageRef) -> u64 {
        page.raw.0
    }
}

impl From<PageRef<'_>> for PagePtr {
    fn from(page: PageRef) -> Self {
        PagePtr::Mem(page.raw.0)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PageKind(u8);

const PAGE_KIND_MASK: u8 = 1 << 7;

impl PageKind {
    pub fn is_leaf(self) -> bool {
        self.0 & PAGE_KIND_MASK == 0
    }

    pub fn subkind(self) -> PageSubKind {
        self.0.into()
    }
}

impl From<u8> for PageKind {
    fn from(kind: u8) -> Self {
        Self(kind)
    }
}

impl From<PageKind> for u8 {
    fn from(kind: PageKind) -> u8 {
        kind.0
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageSubKind {
    Data = 0,
    Split = 1,
}

impl From<u8> for PageSubKind {
    fn from(kind: u8) -> Self {
        match kind & !PAGE_KIND_MASK {
            0 => Self::Data,
            1 => Self::Split,
            _ => panic!("invalid page sub kind"),
        }
    }
}

// A page allocator.
pub trait PageAlloc {
    unsafe fn alloc_page(&self, size: usize) -> Option<PageBuf>;

    unsafe fn dealloc_page(&self, page: PageBuf);
}

impl<T: GlobalAlloc> PageAlloc for T {
    unsafe fn alloc_page(&self, size: usize) -> Option<PageBuf> {
        let layout = alloc_layout(size);
        let ptr = self.alloc(layout);
        if ptr.is_null() {
            None
        } else {
            Some(PageBuf::from_raw(ptr, size))
        }
    }

    unsafe fn dealloc_page(&self, page: PageBuf) {
        let layout = alloc_layout(page.size());
        self.dealloc(page.into_raw(), layout);
    }
}

unsafe fn alloc_layout(size: usize) -> Layout {
    Layout::from_size_align_unchecked(size, PAGE_HEADER_ALIGNMENT)
}

// Page header: | ver (6B) | len (1B) | kind (1B) | next (8B) |
pub(super) const PAGE_HEADER_SIZE: usize = 16;
pub(super) const PAGE_HEADER_ALIGNMENT: usize = 8;

// An internal type to do unsafe operations on a page.
#[derive(Copy, Clone, Debug)]
struct RawPage(u64);

// TODO: handle endianness
impl RawPage {
    fn ver(&self) -> u64 {
        unsafe {
            let ptr = self.0 as *const u64;
            ptr.read() >> 16
        }
    }

    fn set_ver(&mut self, ver: u64) {
        unsafe {
            let ptr = self.0 as *mut u64;
            ptr.write(ver << 16 | (self.len() as u64) << 8 | self.kind() as u64);
        }
    }

    fn len(&self) -> u8 {
        unsafe {
            let ptr = self.0 as *const u8;
            ptr.add(6).read()
        }
    }

    fn set_len(&mut self, len: u8) {
        unsafe {
            let ptr = self.0 as *mut u8;
            ptr.add(6).write(len);
        }
    }

    fn kind(&self) -> u8 {
        unsafe {
            let ptr = self.0 as *const u8;
            ptr.add(7).read()
        }
    }

    fn set_kind(&mut self, kind: u8) {
        unsafe {
            let ptr = self.0 as *mut u8;
            ptr.add(7).write(kind);
        }
    }

    fn next(&self) -> PagePtr {
        unsafe {
            let ptr = self.0 as *const u64;
            ptr.add(1).read().into()
        }
    }

    fn set_next(&mut self, next: PagePtr) {
        unsafe {
            let ptr = self.0 as *mut u64;
            ptr.add(1).write(next.into());
        }
    }

    fn content(&self) -> *const u8 {
        unsafe {
            let ptr = self.0 as *const u8;
            ptr.add(PAGE_HEADER_SIZE)
        }
    }

    fn content_mut(&mut self) -> *mut u8 {
        unsafe {
            let ptr = self.0 as *mut u8;
            ptr.add(PAGE_HEADER_SIZE)
        }
    }
}
