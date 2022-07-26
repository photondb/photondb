use std::{alloc::Layout, marker::PhantomData, ptr::NonNull};

use bitflags::bitflags;

// Page header: ver (6B) | len (1B) | tags (1B) | next (8B) | freq (4B) |
const PAGE_ALIGNMENT: usize = 8;
const PAGE_VERSION_SIZE: usize = 6;
pub const PAGE_HEADER_SIZE: usize = 20;

#[derive(Copy, Clone, Debug)]
pub struct PagePtr(NonNull<u8>);

impl PagePtr {
    pub unsafe fn new(ptr: *mut u8) -> Option<Self> {
        NonNull::new(ptr).map(PagePtr)
    }

    pub const fn as_ptr(self) -> *mut u8 {
        self.0.as_ptr()
    }

    unsafe fn ver_ptr(self) -> *mut u8 {
        self.as_ptr()
    }

    unsafe fn len_ptr(self) -> *mut u8 {
        self.as_ptr().add(1)
    }

    unsafe fn tags_ptr(self) -> *mut u8 {
        self.as_ptr().add(PAGE_VERSION_SIZE + 1)
    }

    unsafe fn next_ptr(self) -> *mut u64 {
        (self.as_ptr() as *mut u64).add(1)
    }

    unsafe fn freq_ptr(self) -> *mut u32 {
        (self.as_ptr() as *mut u32).add(4)
    }

    unsafe fn content_ptr(self) -> *mut u8 {
        self.as_ptr().add(PAGE_HEADER_SIZE)
    }

    pub fn ver(&self) -> u64 {
        unsafe {
            let mut ver = 0u64;
            let ver_ptr = &mut ver as *mut u64 as *mut u8;
            ver_ptr.copy_from_nonoverlapping(self.ver_ptr(), PAGE_VERSION_SIZE);
            u64::from_le(ver)
        }
    }

    pub fn set_ver(&mut self, ver: u64) {
        unsafe {
            let ver = ver.to_le();
            let ver_ptr = &ver as *const u64 as *const u8;
            ver_ptr.copy_to_nonoverlapping(self.ver_ptr(), PAGE_VERSION_SIZE);
        }
    }

    // Returns the length of the chain.
    pub fn len(&self) -> u8 {
        unsafe { self.len_ptr().read() }
    }

    pub fn set_len(&mut self, len: u8) {
        unsafe {
            self.len_ptr().write(len);
        }
    }

    pub fn tags(&self) -> PageTags {
        unsafe {
            let bits = self.tags_ptr().read();
            PageTags::from_bits(bits).unwrap()
        }
    }

    pub fn set_tags(&mut self, tags: PageTags) {
        unsafe {
            self.tags_ptr().write(tags.bits());
        }
    }

    // Returns the address of the next page.
    pub fn next(&self) -> u64 {
        unsafe { self.next_ptr().read().to_le() }
    }

    pub fn set_next(&mut self, next: u64) {
        unsafe {
            self.next_ptr().write(next.to_le());
        }
    }

    pub fn content(&self) -> *const u8 {
        unsafe { self.content_ptr() }
    }

    pub fn content_mut(&mut self) -> *mut u8 {
        unsafe { self.content_ptr() }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct PageRef<'a> {
    ptr: PagePtr,
    _mark: PhantomData<&'a [u8]>,
}

impl PageRef<'_> {
    pub unsafe fn new(ptr: *const u8) -> Option<Self> {
        PagePtr::new(ptr as *mut u8).map(Self::from)
    }

    pub const fn as_ptr(self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn tags(&self) -> PageTags {
        self.ptr.tags()
    }

    pub fn ver(&self) -> u64 {
        self.ptr.ver()
    }

    pub fn len(&self) -> u8 {
        self.ptr.len()
    }

    pub fn next(&self) -> u64 {
        self.ptr.next()
    }

    pub fn content(&self) -> *const u8 {
        self.ptr.content()
    }
}

impl From<PagePtr> for PageRef<'_> {
    fn from(ptr: PagePtr) -> Self {
        Self {
            ptr,
            _mark: PhantomData,
        }
    }
}

bitflags! {
    #[derive(Default)]
    pub struct PageTags: u8 {
        const LEAF  = 0b10000000;
        const DATA  = 0b00000000;
        const SPLIT = 0b00000001;
    }
}

impl PageTags {
    pub fn as_kind(self) -> Self {
        self & !Self::LEAF
    }

    pub const fn is_leaf(self) -> bool {
        self.contains(Self::LEAF)
    }
}

pub unsafe trait PageAlloc {
    unsafe fn alloc(&self, size: usize) -> Option<PagePtr>;

    unsafe fn dealloc(&self, page: PagePtr);

    unsafe fn alloc_layout(size: usize) -> Layout {
        Layout::from_size_align_unchecked(size, PAGE_ALIGNMENT)
    }
}
