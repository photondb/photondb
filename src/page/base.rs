use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use bitflags::bitflags;

/// Page header format {
///     flags      : 1B
///     epoch      : 6B
///     chain_len  : 1B
///     chain_next : 8B
/// }
#[derive(Copy, Clone)]
pub(crate) struct PagePtr {
    ptr: NonNull<u8>,
    len: usize,
}

impl PagePtr {
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
    pub(crate) fn epoch(&self) -> PageEpoch {
        todo!()
    }

    /// Updates the page epoch.
    pub(crate) fn set_epoch(&mut self, epoch: PageEpoch) {
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

impl<'a> From<PageBuf<'a>> for PageRef<'a> {
    fn from(buf: PageBuf<'a>) -> Self {
        todo!()
    }
}

/// An immutable reference to a page.
#[derive(Copy, Clone)]
pub(crate) struct PageRef<'a> {
    ptr: PagePtr,
    _marker: PhantomData<&'a ()>,
}

impl<'a> Deref for PageRef<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum PageKind {
    Data,
    Split,
}

bitflags! {
    struct PageFlags: u8 {
        const LEAF = 0b0000_0001;
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PageEpoch(u64);

const PAGE_EPOCH_MAX: u64 = (1 << 48) - 1;

impl PageEpoch {
    pub(crate) fn next(self) -> Self {
        assert!(self.0 < PAGE_EPOCH_MAX);
        Self(self.0 + 1)
    }
}

pub(super) struct PageBuilder {
    tier: PageTier,
    kind: PageKind,
}

impl PageBuilder {
    pub(super) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self { tier, kind }
    }
}
