use std::{
    fmt,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

/// Page format {
///     epoch      : 6 bytes
///     flags      : 1 bytes
///     chain_len  : 1 bytes
///     chain_next : 8 bytes
///     content    : multiple bytes
/// }
const PAGE_EPOCH_MAX: u64 = (1 << 48) - 1;
const PAGE_EPOCH_LEN: usize = 6;
const PAGE_HEADER_LEN: usize = 16;

/// A raw pointer to a page.
#[derive(Copy, Clone)]
pub(crate) struct PagePtr {
    ptr: NonNull<u8>,
    len: usize,
}

impl PagePtr {
    pub(crate) fn new(ptr: NonNull<u8>, len: usize) -> Self {
        PagePtr { ptr, len }
    }

    /// Returns the page size.
    pub(crate) fn size(&self) -> usize {
        self.len
    }

    /// Returns the page tier.
    pub(crate) fn tier(&self) -> PageTier {
        self.flags().tier()
    }

    /// Returns the page kind.
    pub(crate) fn kind(&self) -> PageKind {
        self.flags().kind()
    }

    /// Returns the page epoch.
    pub(crate) fn epoch(&self) -> u64 {
        unsafe {
            let ptr = self.epoch_ptr() as *mut u64;
            let val = u64::from_le(ptr.read());
            val & PAGE_EPOCH_MAX
        }
    }

    /// Updates the page epoch.
    ///
    /// # Panics
    ///
    /// This function panics if the epoch is greater than `PAGE_EPOCH_MAX`.
    pub(crate) fn set_epoch(&mut self, epoch: u64) {
        assert!(epoch <= PAGE_EPOCH_MAX);
        unsafe {
            let val = epoch.to_le();
            let val_ptr = &val as *const u64 as *const u8;
            val_ptr.copy_to_nonoverlapping(self.epoch_ptr(), PAGE_EPOCH_LEN);
        }
    }

    /// Returns the length of the chain.
    pub(crate) fn chain_len(&self) -> u8 {
        unsafe { self.chain_len_ptr().read() }
    }

    /// Updates the length of the chain.
    pub(crate) fn set_chain_len(&self, len: u8) {
        unsafe { self.chain_len_ptr().write(len) }
    }

    /// Returns the address of the next page.
    pub(crate) fn chain_next(&self) -> u64 {
        unsafe { self.chain_next_ptr().read() }
    }

    /// Updates the address of the next page.
    pub(crate) fn set_chain_next(&mut self, addr: u64) {
        unsafe { self.chain_next_ptr().write(addr) }
    }
}

impl PagePtr {
    fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    unsafe fn epoch_ptr(&self) -> *mut u8 {
        self.as_ptr()
    }

    unsafe fn flags_ptr(&self) -> *mut u8 {
        self.as_ptr().add(PAGE_EPOCH_LEN)
    }

    unsafe fn chain_len_ptr(&self) -> *mut u8 {
        self.as_ptr().add(PAGE_EPOCH_LEN + 1)
    }

    unsafe fn chain_next_ptr(&self) -> *mut u64 {
        (self.as_ptr() as *mut u64).add(1)
    }

    fn flags(&self) -> PageFlags {
        unsafe { PageFlags(self.flags_ptr().read()) }
    }

    fn set_flags(&mut self, flags: PageFlags) {
        unsafe { self.flags_ptr().write(flags.0) }
    }
}

impl fmt::Debug for PagePtr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Page")
            .field("tier", &self.tier())
            .field("kind", &self.kind())
            .field("epoch", &self.epoch())
            .field("chain_len", &self.chain_len())
            .field("chain_next", &self.chain_next())
            .finish()
    }
}

/// A mutable reference to a page.
pub(crate) struct PageBuf<'a> {
    ptr: PagePtr,
    _marker: PhantomData<&'a mut [u8]>,
}

impl<'a> PageBuf<'a> {
    pub(crate) fn new(ptr: PagePtr) -> Self {
        PageBuf {
            ptr,
            _marker: PhantomData,
        }
    }
}

impl<'a> Deref for PageBuf<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<'a> DerefMut for PageBuf<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ptr
    }
}

impl<'a> From<&'a mut [u8]> for PageBuf<'a> {
    fn from(buf: &'a mut [u8]) -> Self {
        let ptr = unsafe { NonNull::new_unchecked(buf.as_mut_ptr()) };
        Self::new(PagePtr::new(ptr, buf.len()))
    }
}

/// An immutable reference to a page.
#[derive(Copy, Clone)]
pub(crate) struct PageRef<'a> {
    ptr: PagePtr,
    _marker: PhantomData<&'a [u8]>,
}

impl<'a> PageRef<'a> {
    pub(crate) fn new(ptr: PagePtr) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        // Safety:
        // 1. the entire range of memory to access is valid and initialized.
        // 2. there no any mutable references pointer to the target range.
        unsafe { std::slice::from_raw_parts(self.ptr.ptr.as_ptr(), self.len) }
    }
}

impl<'a> Deref for PageRef<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<'a> From<&'a [u8]> for PageRef<'a> {
    fn from(buf: &'a [u8]) -> Self {
        let ptr = unsafe { NonNull::new_unchecked(buf.as_ptr() as *mut _) };
        Self::new(PagePtr::new(ptr, buf.len()))
    }
}

impl<'a> From<PageBuf<'a>> for PageRef<'a> {
    fn from(buf: PageBuf<'a>) -> Self {
        Self::new(buf.ptr)
    }
}

/// A page is either a leaf page or an inner page.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum PageTier {
    Leaf = PAGE_TIER_LEAF,
    Inner = PAGE_TIER_INNER,
}

const PAGE_TIER_MASK: u8 = 0b0000_0001;
const PAGE_TIER_LEAF: u8 = 0b0000_0000;
const PAGE_TIER_INNER: u8 = 0b0000_0001;

impl PageTier {
    pub(crate) fn is_leaf(&self) -> bool {
        self == &Self::Leaf
    }

    pub(crate) fn is_inner(&self) -> bool {
        self == &Self::Inner
    }
}

impl From<u8> for PageTier {
    fn from(value: u8) -> Self {
        match value & PAGE_TIER_MASK {
            PAGE_TIER_LEAF => Self::Leaf,
            PAGE_TIER_INNER => Self::Inner,
            _ => unreachable!(),
        }
    }
}

/// A list of possible page kinds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum PageKind {
    Data = PAGE_KIND_DATA,
    Split = PAGE_KIND_SPLIT,
}

const PAGE_KIND_MASK: u8 = 0b0000_1110;
const PAGE_KIND_DATA: u8 = 0b0000_0000;
const PAGE_KIND_SPLIT: u8 = 0b0000_0010;

impl PageKind {
    pub(crate) fn is_data(&self) -> bool {
        self == &Self::Data
    }

    pub(crate) fn is_split(&self) -> bool {
        self == &Self::Split
    }
}

impl From<u8> for PageKind {
    fn from(value: u8) -> Self {
        match value & PAGE_KIND_MASK {
            PAGE_KIND_DATA => Self::Data,
            PAGE_KIND_SPLIT => Self::Split,
            _ => unreachable!(),
        }
    }
}

struct PageFlags(u8);

impl PageFlags {
    fn new(tier: PageTier, kind: PageKind) -> Self {
        Self(tier as u8 | kind as u8)
    }

    fn tier(&self) -> PageTier {
        self.0.into()
    }

    fn kind(&self) -> PageKind {
        self.0.into()
    }
}

/// Builds a page with basic information.
pub(crate) struct PageBuilder {
    tier: PageTier,
    kind: PageKind,
}

impl PageBuilder {
    pub(crate) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self { tier, kind }
    }

    pub(crate) fn build(&self, page: &mut PageBuf<'_>) {
        let flags = PageFlags::new(self.tier, self.kind);
        page.set_flags(flags);
        page.set_epoch(0);
        page.set_chain_len(1);
        page.set_chain_next(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page() {
        let mut buf = [0u8; PAGE_HEADER_LEN];
        let mut page = PageBuf::from(buf.as_mut_slice());
        {
            let builder = PageBuilder::new(PageTier::Leaf, PageKind::Data);
            builder.build(&mut page);
            assert_eq!(page.tier(), PageTier::Leaf);
            assert_eq!(page.kind(), PageKind::Data);
        }
        {
            let builder = PageBuilder::new(PageTier::Inner, PageKind::Split);
            builder.build(&mut page);
            assert_eq!(page.tier(), PageTier::Inner);
            assert_eq!(page.kind(), PageKind::Split);
        }

        assert_eq!(page.epoch(), 0);
        page.set_epoch(1);
        assert_eq!(page.epoch(), 1);
        assert_eq!(page.chain_len(), 1);
        page.set_chain_len(2);
        assert_eq!(page.chain_len(), 2);
        assert_eq!(page.chain_next(), 0);
        page.set_chain_next(3);
        assert_eq!(page.chain_next(), 3);
    }
}
