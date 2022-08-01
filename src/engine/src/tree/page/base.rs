use std::{alloc::Layout, ptr::NonNull};

// Page header: ver (6B) | rank (1B) | tags (1B) | next (8B) | content_size (4B) |
const PAGE_ALIGNMENT: usize = 8;
const PAGE_HEADER_SIZE: usize = 20;
const PAGE_VERSION_MAX: u64 = (1 << 48) - 1;
const PAGE_VERSION_SIZE: usize = 6;

/// A non-null pointer to a page.
#[derive(Copy, Clone, Debug)]
pub struct PagePtr(NonNull<u8>);

impl PagePtr {
    /// Creates a new `PagePtr` if `ptr` is non-null.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it does not check if `ptr` points to a valid page.
    pub unsafe fn new(ptr: *mut u8) -> Option<Self> {
        NonNull::new(ptr).map(Self)
    }

    /// Returns the raw pointer.
    pub const fn as_raw(self) -> *mut u8 {
        self.0.as_ptr()
    }

    unsafe fn ver_ptr(self) -> *mut u8 {
        self.as_raw()
    }

    unsafe fn rank_ptr(self) -> *mut u8 {
        self.as_raw().add(PAGE_VERSION_SIZE)
    }

    unsafe fn tags_ptr(self) -> *mut u8 {
        self.as_raw().add(PAGE_VERSION_SIZE + 1)
    }

    unsafe fn next_ptr(self) -> *mut u64 {
        (self.as_raw() as *mut u64).add(1)
    }

    unsafe fn content_size_ptr(self) -> *mut u32 {
        (self.as_raw() as *mut u32).add(4)
    }

    /// Returns the page version.
    pub fn ver(&self) -> u64 {
        unsafe {
            let mut ver = 0u64;
            let ver_ptr = &mut ver as *mut u64 as *mut u8;
            ver_ptr.copy_from_nonoverlapping(self.ver_ptr(), PAGE_VERSION_SIZE);
            u64::from_le(ver)
        }
    }

    pub fn set_ver(&mut self, ver: u64) {
        assert!(ver <= PAGE_VERSION_MAX);
        unsafe {
            let ver = ver.to_le();
            let ver_ptr = &ver as *const u64 as *const u8;
            ver_ptr.copy_to_nonoverlapping(self.ver_ptr(), PAGE_VERSION_SIZE);
        }
    }

    /// Returns the rank of the page in the chain.
    pub fn rank(&self) -> u8 {
        unsafe { self.rank_ptr().read() }
    }

    pub fn set_rank(&mut self, rank: u8) {
        unsafe {
            self.rank_ptr().write(rank);
        }
    }

    /// Returns the address of the next page in the chain.
    pub fn next(&self) -> u64 {
        let next = unsafe { self.next_ptr().read() };
        u64::from_le(next)
    }

    pub fn set_next(&mut self, next: u64) {
        unsafe {
            self.next_ptr().write(next.to_le());
        }
    }

    fn tags(&self) -> PageTags {
        unsafe { self.tags_ptr().read().into() }
    }

    fn set_tags(&mut self, tags: PageTags) {
        unsafe {
            self.tags_ptr().write(tags.into());
        }
    }

    /// Returns the page kind.
    pub fn kind(&self) -> PageKind {
        self.tags().kind()
    }

    pub fn set_kind(&mut self, kind: PageKind) {
        self.set_tags(self.tags().with_kind(kind));
    }

    /// Returns true if this is a leaf page.
    pub fn is_leaf(&self) -> bool {
        self.tags().is_leaf()
    }

    pub fn set_leaf(&mut self, is_leaf: bool) {
        self.set_tags(self.tags().with_leaf(is_leaf));
    }

    /// Sets the page header as default.
    pub fn set_default(&mut self) {
        unsafe { self.as_raw().write_bytes(0, PAGE_HEADER_SIZE) };
    }

    /// Returns a pointer to the page content.
    pub fn content(&self) -> *const u8 {
        unsafe { self.as_raw().add(PAGE_HEADER_SIZE) }
    }

    /// Returns a mutable pointer to the page content.
    pub fn content_mut(&mut self) -> *mut u8 {
        unsafe { self.as_raw().add(PAGE_HEADER_SIZE) }
    }

    /// Returns the page size.
    pub fn size(&self) -> usize {
        PAGE_HEADER_SIZE + self.content_size() as usize
    }

    /// Returns the page content size.
    pub fn content_size(&self) -> usize {
        let size = unsafe { self.content_size_ptr().read() };
        u32::from_le(size) as usize
    }

    fn set_content_size(&mut self, size: usize) {
        assert!(size <= u32::MAX as usize);
        unsafe {
            self.content_size_ptr().write((size as u32).to_le());
        }
    }
}

impl From<PagePtr> for u64 {
    fn from(ptr: PagePtr) -> Self {
        ptr.as_raw() as u64
    }
}

#[derive(Copy, Clone, Debug, Default)]
struct PageTags(u8);

const PAGE_KIND_MASK: u8 = 0x7F;

impl PageTags {
    const fn kind(self) -> PageKind {
        PageKind::new(self.0 & PAGE_KIND_MASK)
    }

    const fn with_kind(self, kind: PageKind) -> Self {
        Self(self.leaf() | kind as u8)
    }

    const fn leaf(self) -> u8 {
        self.0 & !PAGE_KIND_MASK
    }

    const fn is_leaf(self) -> bool {
        self.leaf() == 0
    }

    const fn with_leaf(self, is_leaf: bool) -> Self {
        if is_leaf {
            Self(self.0 & PAGE_KIND_MASK)
        } else {
            Self(self.0 | !PAGE_KIND_MASK)
        }
    }
}

impl From<u8> for PageTags {
    fn from(v: u8) -> Self {
        Self(v)
    }
}

impl From<PageTags> for u8 {
    fn from(tags: PageTags) -> Self {
        tags.0
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PageKind {
    /// Pages with data entries.
    Data = 0,
    /// Pages with index entries.
    Index = 1,
    /// Pages with split information.
    Split = 2,
}

impl PageKind {
    const fn new(kind: u8) -> Self {
        match kind {
            0 => Self::Data,
            1 => Self::Index,
            2 => Self::Split,
            _ => panic!("invalid page kind"),
        }
    }
}

impl From<PageKind> for u8 {
    fn from(kind: PageKind) -> Self {
        kind as u8
    }
}

/// An interface to allocate and deallocate pages.
///
/// # Safety
///
/// Similar to the `std::alloc::Allocator` trait.
pub unsafe trait PageAlloc {
    type Error;

    fn alloc(&self, size: usize) -> Result<PagePtr, Self::Error>;

    unsafe fn dealloc(&self, page: PagePtr);

    fn alloc_layout(size: usize) -> Layout {
        unsafe { Layout::from_size_align_unchecked(size, PAGE_ALIGNMENT) }
    }
}

/// A builder to create base pages.
pub struct PageBuilder {
    kind: PageKind,
    is_leaf: bool,
}

impl PageBuilder {
    pub fn new(kind: PageKind, is_leaf: bool) -> Self {
        Self { kind, is_leaf }
    }

    pub fn build<A>(&self, alloc: &A, content_size: usize) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        let ptr = alloc.alloc(PAGE_HEADER_SIZE + content_size);
        ptr.map(|mut ptr| {
            ptr.set_default();
            ptr.set_kind(self.kind);
            ptr.set_leaf(self.is_leaf);
            ptr.set_content_size(content_size);
            ptr
        })
    }
}

#[cfg(test)]
pub mod test {
    use std::alloc::GlobalAlloc;

    use jemallocator::{usable_size, Jemalloc};

    use super::*;

    pub const ALLOC: TestAlloc = TestAlloc;

    pub struct TestAlloc;

    unsafe impl PageAlloc for TestAlloc {
        type Error = ();

        fn alloc(&self, size: usize) -> Result<PagePtr, Self::Error> {
            unsafe {
                let ptr = Jemalloc.alloc(Self::alloc_layout(size));
                PagePtr::new(ptr).ok_or(())
            }
        }

        unsafe fn dealloc(&self, page: PagePtr) {
            let ptr = page.as_raw();
            let size = usable_size(ptr);
            Jemalloc.dealloc(ptr, Self::alloc_layout(size));
        }
    }

    #[test]
    fn page_ptr() {
        let mut buf = [1u8; PAGE_HEADER_SIZE];
        let mut ptr = unsafe { PagePtr::new(buf.as_mut_ptr()).unwrap() };
        ptr.set_default();

        assert_eq!(ptr.ver(), 0);
        ptr.set_ver(1);
        assert_eq!(ptr.ver(), 1);

        assert_eq!(ptr.rank(), 0);
        ptr.set_rank(2);
        assert_eq!(ptr.rank(), 2);

        assert_eq!(ptr.next(), 0);
        ptr.set_next(3);
        assert_eq!(ptr.next(), 3);

        assert_eq!(ptr.kind(), PageKind::Data);
        ptr.set_kind(PageKind::Split);
        assert_eq!(ptr.kind(), PageKind::Split);

        assert_eq!(ptr.is_leaf(), true);
        ptr.set_leaf(false);
        assert_eq!(ptr.is_leaf(), false);

        assert_eq!(ptr.content_size(), 0);
        ptr.set_content_size(4);
        assert_eq!(ptr.content_size(), 4);
    }
}
