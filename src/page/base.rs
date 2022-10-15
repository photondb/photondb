use std::ops::Deref;

use bitflags::bitflags;

/// Page header format {
///     flags      : 1B
///     epoch      : 6B
///     chain_len  : 1B
///     chain_next : 8B
/// }
pub(crate) struct PageBuf;

impl PageBuf {
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

pub(crate) struct PageRef(PageBuf);

impl Deref for PageRef {
    type Target = PageBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) enum PageTier {
    Leaf,
    Inner,
}

pub(crate) enum PageKind {
    Data,
    Split,
}

bitflags! {
    struct PageFlags: u8 {
        const LEAF = 0b0000_0001;
        const KIND = 0b0000_0010;
    }
}

pub(crate) struct PageEpoch(u64);
