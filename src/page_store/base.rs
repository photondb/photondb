use std::sync::atomic::{AtomicU64, Ordering};

/// A unique identifier for a page.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct PageId(u64);

/// An atomic [`PageId`] that allows concurrent access.
pub(crate) struct AtomicPageId(AtomicU64);

impl AtomicPageId {
    pub(crate) fn get(&self) -> PageId {
        PageId(self.0.load(Ordering::Acquire))
    }

    pub(crate) fn set(&self, new: PageId) {
        self.0.store(new.0, Ordering::Release)
    }

    pub(crate) fn cas(&self, old: PageId, new: PageId) -> Result<(), PageId> {
        match self
            .0
            .compare_exchange(old.0, new.0, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(actual) => Err(PageId(actual)),
        }
    }
}

impl From<PageId> for AtomicPageId {
    fn from(id: PageId) -> Self {
        Self(AtomicU64::new(id.0))
    }
}

/// A logical address of a page.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct PageAddr(u64);

impl From<PageAddr> for u64 {
    fn from(addr: PageAddr) -> Self {
        addr.0
    }
}
