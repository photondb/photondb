use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use bitflags::bitflags;

/// Page format {
///     flags      : 1 bytes
///     epoch      : 6 bytes
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
        todo!()
    }

    /// Updates the page tier.
    pub(crate) fn set_tier(&mut self, tier: PageTier) {
        todo!()
    }

    /// Returns the page kind.
    pub(crate) fn kind(&self) -> PageKind {
        todo!()
    }

    /// Updates the page kind.
    pub(crate) fn set_kind(&mut self, kind: PageKind) {
        todo!()
    }

    /// Returns the page epoch.
    pub(crate) fn epoch(&self) -> u64 {
        todo!()
    }

    /// Updates the page epoch.
    pub(crate) fn set_epoch(&mut self, epoch: u64) {
        assert!(epoch <= PAGE_EPOCH_MAX);
        todo!()
    }

    /// Returns the length of the chain.
    pub(crate) fn chain_len(&self) -> u8 {
        todo!()
    }

    /// Updates the length of the chain.
    pub(crate) fn set_chain_len(&self, len: u8) {
        todo!()
    }

    /// Returns the address of the next page.
    pub(crate) fn chain_next(&self) -> u64 {
        todo!()
    }

    /// Updates the address of the next page.
    pub(crate) fn set_chain_next(&mut self, addr: u64) {
        todo!()
    }
}

/// A mutable reference to a page.
pub(crate) struct PageBuf<'a> {
    ptr: PagePtr,
    _marker: PhantomData<&'a ()>,
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

/// An immutable reference to a page.
#[derive(Copy, Clone)]
pub(crate) struct PageRef<'a> {
    ptr: PagePtr,
    _marker: PhantomData<&'a ()>,
}

impl<'a> PageRef<'a> {
    pub(crate) fn new(ptr: PagePtr) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }
}

impl<'a> Deref for PageRef<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<'a> From<PageBuf<'a>> for PageRef<'a> {
    fn from(buf: PageBuf<'a>) -> Self {
        Self::new(buf.ptr)
    }
}

/// A page is either a leaf page or an inner page.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum PageTier {
    Leaf,
    Inner,
}

impl PageTier {
    pub(crate) fn is_leaf(&self) -> bool {
        self == &Self::Leaf
    }

    pub(crate) fn is_inner(&self) -> bool {
        self == &Self::Inner
    }
}

/// A list of possible page kinds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum PageKind {
    Data,
    Split,
}

impl PageKind {
    pub(crate) fn is_data(&self) -> bool {
        self == &Self::Data
    }

    pub(crate) fn is_split(&self) -> bool {
        self == &Self::Split
    }
}

bitflags! {
    struct PageFlags: u8 {
        const LEAF = 0b0000_0001;
    }
}

/// Builds a page with basic information.
pub(crate) struct PageBuilder {
    tier: PageTier,
    kind: PageKind,
    epoch: u64,
    chain_len: u8,
    chain_next: u64,
}

impl PageBuilder {
    pub(crate) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self {
            tier,
            kind,
            epoch: 0,
            chain_len: 1,
            chain_next: 0,
        }
    }

    pub(crate) fn epoch(mut self, epoch: u64) -> Self {
        self.epoch = epoch;
        self
    }

    pub(crate) fn chain_len(mut self, chain_len: u8) -> Self {
        self.chain_len = chain_len;
        self
    }

    pub(crate) fn chain_next(mut self, chain_next: u64) -> Self {
        self.chain_next = chain_next;
        self
    }

    pub(crate) fn build(&self, page: &mut PageBuf<'_>) {
        page.set_tier(self.tier);
        page.set_kind(self.kind);
        page.set_epoch(self.epoch);
        page.set_chain_len(self.chain_len);
        page.set_chain_next(self.chain_next);
    }
}
