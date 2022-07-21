use std::{
    alloc::{GlobalAlloc, Layout},
    marker::PhantomData,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PagePtr {
    Mem(u64),
    Disk(u64),
}

const MEM_DISK_MASK: u64 = 1 << 63;

impl From<u64> for PagePtr {
    fn from(addr: u64) -> Self {
        assert!(addr != 0);
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

pub struct PageBuf {
    raw: RawPage,
    size: usize,
}

impl PageBuf {
    unsafe fn from_raw(ptr: *mut u8, size: usize) -> Self {
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
        self.raw.kind()
    }

    pub fn set_kind(&mut self, kind: PageKind) {
        self.raw.set_kind(kind);
    }

    pub fn set_next(&mut self, next: PagePtr) {
        self.raw.set_next(next.into());
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
        self.raw.kind()
    }

    pub fn next(&self) -> Option<PagePtr> {
        let ptr = self.raw.next();
        if ptr == 0 {
            None
        } else {
            Some(ptr.into())
        }
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageKind {
    Data = 0,
    Index = 1,
}

impl PageKind {
    pub fn is_data(self) -> bool {
        self < Self::Index
    }
}

impl From<u8> for PageKind {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Data,
            1 => Self::Index,
            _ => panic!("invalid page kind"),
        }
    }
}

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

    fn kind(&self) -> PageKind {
        unsafe {
            let ptr = self.0 as *const u8;
            ptr.add(7).read().into()
        }
    }

    fn set_kind(&mut self, kind: PageKind) {
        unsafe {
            let ptr = self.0 as *mut u8;
            ptr.add(7).write(kind as u8);
        }
    }

    fn next(&self) -> u64 {
        unsafe {
            let ptr = self.0 as *const u64;
            ptr.add(1).read()
        }
    }

    fn set_next(&mut self, next: u64) {
        unsafe {
            let ptr = self.0 as *mut u64;
            ptr.add(1).write(next);
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
