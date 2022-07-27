use std::{marker::PhantomData, ptr::NonNull};

// Page header: ver (6B) | tag (1B) | len (1B) | next (8B) | freq (4B) |
pub const PAGE_ALIGNMENT: usize = 8;
pub const PAGE_HEADER_SIZE: usize = 20;
pub const PAGE_VERSION_SIZE: usize = 6;

#[derive(Copy, Clone, Debug)]
pub struct PagePtr(NonNull<u8>);

impl PagePtr {
    pub unsafe fn new(ptr: *mut u8) -> Option<Self> {
        NonNull::new(ptr).map(Self)
    }

    pub const fn as_ptr(self) -> *mut u8 {
        self.0.as_ptr()
    }

    unsafe fn ver_ptr(self) -> *mut u8 {
        self.as_ptr()
    }

    unsafe fn tag_ptr(self) -> *mut u8 {
        self.as_ptr().add(PAGE_VERSION_SIZE)
    }

    unsafe fn len_ptr(self) -> *mut u8 {
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

    fn tag(&self) -> PageTag {
        unsafe { self.tag_ptr().read().into() }
    }

    fn set_tag(&mut self, tag: PageTag) {
        unsafe {
            self.tag_ptr().write(tag.into());
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

    // Returns the address of the next page in the chain.
    pub fn next(&self) -> u64 {
        unsafe { self.next_ptr().read().to_le() }
    }

    pub fn set_next(&mut self, next: u64) {
        unsafe {
            self.next_ptr().write(next.to_le());
        }
    }

    pub fn kind(&self) -> PageKind {
        self.tag().kind()
    }

    pub fn set_kind(&mut self, kind: PageKind) {
        self.set_tag(self.tag().set_kind(kind));
    }

    pub fn is_leaf(&self) -> bool {
        self.tag().is_leaf()
    }

    pub fn set_leaf(&mut self, v: bool) {
        self.set_tag(self.tag().set_leaf(v));
    }

    pub fn set_default(&mut self) {
        unsafe { self.as_ptr().write_bytes(0, PAGE_HEADER_SIZE) };
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

    pub fn ver(self) -> u64 {
        self.ptr.ver()
    }

    pub fn len(self) -> u8 {
        self.ptr.len()
    }

    pub fn next(self) -> u64 {
        self.ptr.next()
    }

    pub fn kind(self) -> PageKind {
        self.ptr.kind()
    }

    pub fn is_leaf(self) -> bool {
        self.ptr.is_leaf()
    }

    pub fn content(self) -> *const u8 {
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

#[derive(Copy, Clone, Default)]
struct PageTag(u8);

const PAGE_LEAF_MASK: u8 = 1 << 7;

impl PageTag {
    fn kind(self) -> PageKind {
        (self.0 & !PAGE_LEAF_MASK).into()
    }

    fn set_kind(self, kind: PageKind) -> Self {
        Self(self.0 | kind as u8)
    }

    fn is_leaf(self) -> bool {
        self.0 & PAGE_LEAF_MASK == 1
    }

    fn set_leaf(self, leaf: bool) -> Self {
        if leaf {
            Self(self.0 | PAGE_LEAF_MASK)
        } else {
            Self(self.0 & !PAGE_LEAF_MASK)
        }
    }
}

impl From<u8> for PageTag {
    fn from(tag: u8) -> Self {
        Self(tag)
    }
}

impl From<PageTag> for u8 {
    fn from(tag: PageTag) -> Self {
        tag.0
    }
}

#[repr(u8)]
pub enum PageKind {
    Data = 0,
    Split = 1,
}

impl From<u8> for PageKind {
    fn from(kind: u8) -> Self {
        match kind {
            0 => Self::Data,
            1 => Self::Split,
            _ => panic!("invalid page kind"),
        }
    }
}

impl From<PageKind> for u8 {
    fn from(kind: PageKind) -> Self {
        kind as u8
    }
}

pub unsafe trait PageAlloc {
    unsafe fn alloc(&self, size: usize) -> Option<PagePtr>;

    unsafe fn dealloc(&self, page: PagePtr);
}
