use std::{alloc::Layout, marker::PhantomData};

use bitflags::bitflags;

// Page layout: | tags (1B) | ver (6B) | len (1B) | next (8B) | content |
const PAGE_ALIGNMENT: usize = 8;
const PAGE_HEADER_SIZE: usize = 16;

#[derive(Copy, Clone, Debug)]
pub struct PagePtr(*mut u8);

impl PagePtr {
    unsafe fn from_raw(ptr: *mut u8) -> Self {
        Self(ptr)
    }

    unsafe fn into_raw(self) -> *mut u8 {
        self.0
    }

    unsafe fn ver_ptr(&self) -> *mut u8 {
        self.0.add(1)
    }

    unsafe fn len_ptr(&self) -> *mut u8 {
        self.0.add(2)
    }

    unsafe fn next_ptr(&self) -> *mut u64 {
        (self.0 as *mut u64).add(1)
    }

    unsafe fn content_ptr(&self) -> *mut u8 {
        self.0.add(PAGE_HEADER_SIZE)
    }

    pub fn tags(&self) -> PageTags {
        unsafe {
            let bits = self.0.read();
            PageTags::from_bits_truncate(bits)
        }
    }

    pub fn set_tags(&mut self, tags: PageTags) {
        unsafe {
            self.0.write(tags.bits());
        }
    }

    // Returns the version number.
    pub fn ver(&self) -> u64 {
        unsafe {
            let mut ver = 0u64;
            let ver_ptr = &mut ver as *mut u64 as *mut u8;
            ver_ptr.copy_from_nonoverlapping(self.ver_ptr(), 6);
            u64::from_le(ver)
        }
    }

    pub fn set_ver(&mut self, ver: u64) {
        unsafe {
            let ver = ver.to_le();
            let ver_ptr = &ver as *const u64 as *const u8;
            ver_ptr.copy_to_nonoverlapping(self.ver_ptr(), 6);
        }
    }

    // Returns the length of the delta chain.
    pub fn len(&self) -> u8 {
        unsafe { self.len_ptr().read() }
    }

    pub fn set_len(&mut self, len: u8) {
        unsafe {
            self.len_ptr().write(len);
        }
    }

    // Returns the address of the next page in the delta chain.
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
    _mark: PhantomData<&'a ()>,
}

impl PageRef<'_> {
    fn new(ptr: PagePtr) -> Self {
        Self {
            ptr,
            _mark: PhantomData,
        }
    }

    pub fn tags(&self) -> PageTags {
        self.ptr.tags()
    }

    pub fn len(&self) -> u8 {
        self.ptr.len()
    }

    pub fn ver(&self) -> u64 {
        self.ptr.ver()
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
        Self::new(ptr)
    }
}

bitflags! {
    pub struct PageTags: u8 {
        const LEAF  = 0b10000000;
        const DATA  = 0b00000000;
        const SPLIT = 0b00000001;
    }
}

impl PageTags {
    pub const fn is_leaf(self) -> bool {
        self.contains(Self::LEAF)
    }

    pub fn as_kind(self) -> Self {
        self & !Self::LEAF
    }
}

pub struct PageAlloc<A: Allocator>(A);

impl<A: Allocator> PageAlloc<A> {
    unsafe fn alloc_page(&self, content_size: usize) -> PagePtr {
        let size = PAGE_HEADER_SIZE + content_size;
        let ptr = self.0.alloc(alloc_layout(size));
        PagePtr::from_raw(ptr)
    }

    unsafe fn dealloc_page(&self, page: PagePtr) {
        let ptr = page.into_raw();
        self.0.dealloc(ptr, PAGE_ALIGNMENT);
    }
}

pub unsafe trait Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8;

    unsafe fn dealloc(&self, ptr: *mut u8, align: usize);
}

unsafe fn alloc_layout(size: usize) -> Layout {
    Layout::from_size_align_unchecked(size, PAGE_ALIGNMENT)
}
