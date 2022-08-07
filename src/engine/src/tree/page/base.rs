use std::{alloc::Layout, fmt, marker::PhantomData, ops::Deref, ptr::NonNull};

// Page header: ver (6B) | rank (1B) | tags (1B) | next (8B) | content_size (4B) |
const PAGE_ALIGNMENT: usize = 8;
const PAGE_HEADER_SIZE: usize = 20;
const PAGE_VERSION_MAX: u64 = (1 << 48) - 1;
const PAGE_VERSION_SIZE: usize = 6;

/// A non-null pointer to a page.
#[derive(Copy, Clone)]
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

    /// Returns the raw pointer to this page.
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

    /// Returns the version of this page.
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

    /// Returns the rank of this page in the chain.
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

    /// Returns the `PageKind` of this page.
    pub fn kind(&self) -> PageKind {
        self.tags().kind()
    }

    pub fn set_kind(&mut self, kind: PageKind) {
        self.set_tags(self.tags().with_kind(kind));
    }

    /// Returns true if this page is a data page.
    pub fn is_data(&self) -> bool {
        self.tags().is_data()
    }

    pub fn set_data(&mut self, is_data: bool) {
        self.set_tags(self.tags().with_data(is_data));
    }

    /// Sets the header of this page to default.
    pub fn set_default(&mut self) {
        unsafe { self.as_raw().write_bytes(0, PAGE_HEADER_SIZE) };
    }

    /// Returns the size of this page.
    pub fn size(&self) -> usize {
        PAGE_HEADER_SIZE + self.content_size() as usize
    }

    /// Returns the content size of this page.
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

    /// Returns a raw pointer to the content of this page.
    pub fn content(&self) -> *const u8 {
        unsafe { self.as_raw().add(PAGE_HEADER_SIZE) }
    }

    /// Returns a mutable pointer to the content of this page.
    pub fn content_mut(&mut self) -> *mut u8 {
        unsafe { self.as_raw().add(PAGE_HEADER_SIZE) }
    }
}

impl From<PagePtr> for u64 {
    fn from(ptr: PagePtr) -> Self {
        ptr.as_raw() as u64
    }
}

impl fmt::Debug for PagePtr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Page")
            .field("ver", &self.ver())
            .field("rank", &self.rank())
            .field("next", &self.next())
            .field("kind", &self.kind())
            .field("is_data", &self.is_data())
            .field("content_size", &self.content_size())
            .finish()
    }
}

/// An immutable reference to a page.
#[derive(Copy, Clone)]
pub struct PageRef<'a> {
    ptr: PagePtr,
    _mark: PhantomData<&'a ()>,
}

impl PageRef<'_> {
    /// Creates a new `PageRef` if `ptr` is non-null.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it does not check if `ptr` points to a valid page.
    pub unsafe fn new(ptr: *mut u8) -> Option<Self> {
        PagePtr::new(ptr).map(Self::from)
    }
}

impl Deref for PageRef<'_> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
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

impl From<PageRef<'_>> for u64 {
    fn from(page: PageRef<'_>) -> Self {
        page.ptr.into()
    }
}

impl fmt::Debug for PageRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.ptr.fmt(f)
    }
}

#[derive(Copy, Clone, Debug, Default)]
struct PageTags(u8);

/// The most significant bit indicates whether this page is a data page.
const PAGE_KIND_MASK: u8 = 0x7F;

impl PageTags {
    const fn kind(self) -> PageKind {
        PageKind::new(self.0 & PAGE_KIND_MASK)
    }

    const fn with_kind(self, kind: PageKind) -> Self {
        Self(self.data() | kind as u8)
    }

    const fn data(self) -> u8 {
        self.0 & !PAGE_KIND_MASK
    }

    const fn is_data(self) -> bool {
        self.data() == 0
    }

    const fn with_data(self, is_data: bool) -> Self {
        if is_data {
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

/// A list of possible page kinds.
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

    fn alloc_page(&self, size: usize) -> Result<PagePtr, Self::Error>;

    unsafe fn dealloc_page(&self, page: PagePtr);

    fn page_layout(size: usize) -> Layout {
        unsafe { Layout::from_size_align_unchecked(size, PAGE_ALIGNMENT) }
    }
}

unsafe impl<T> PageAlloc for T
where
    T: crate::util::SizedAlloc,
{
    type Error = ();

    fn alloc_page(&self, size: usize) -> Result<PagePtr, Self::Error> {
        unsafe {
            let ptr = self.alloc(Self::page_layout(size));
            PagePtr::new(ptr).ok_or(())
        }
    }

    unsafe fn dealloc_page(&self, page: PagePtr) {
        let ptr = page.as_raw();
        let size = self.alloc_size(ptr);
        self.dealloc(ptr, Self::page_layout(size));
    }
}

/// A builder to create base pages.
pub struct PageBuilder {
    kind: PageKind,
    is_data: bool,
}

impl PageBuilder {
    pub fn new(kind: PageKind, is_data: bool) -> Self {
        Self { kind, is_data }
    }

    pub fn build<A>(&self, alloc: &A, content_size: usize) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        let ptr = alloc.alloc_page(PAGE_HEADER_SIZE + content_size);
        ptr.map(|mut ptr| {
            ptr.set_default();
            ptr.set_kind(self.kind);
            ptr.set_data(self.is_data);
            ptr.set_content_size(content_size);
            ptr
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::util::Sysalloc;

    #[test]
    fn page() {
        let mut ptr = Sysalloc.alloc_page(PAGE_HEADER_SIZE).unwrap();
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

        assert_eq!(ptr.is_data(), true);
        ptr.set_data(false);
        assert_eq!(ptr.is_data(), false);

        assert_eq!(ptr.content_size(), 0);
        ptr.set_content_size(4);
        assert_eq!(ptr.content_size(), 4);
    }
}
