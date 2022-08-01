use std::ops::Deref;

use super::*;

/// A builder to create index pages.
pub struct IndexPageBuilder(SortedPageBuilder);

impl Default for IndexPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::new(PageKind::Index, false))
    }
}

impl IndexPageBuilder {
    /// Builds an index page with entries from the given iterator.
    pub fn build_from_iter<'a, A, I>(self, alloc: &A, iter: &mut I) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
        I: RewindableIter<Key = &'a [u8], Value = Index>,
    {
        self.0.build_from_iter(alloc, iter)
    }
}

/// An immutable reference to an index page.
#[derive(Clone)]
pub struct IndexPageRef<'a>(SortedPageRef<'a, &'a [u8], Index>);

impl<'a> IndexPageRef<'a> {
    pub fn new(ptr: PagePtr) -> Self {
        assert_eq!(ptr.kind(), PageKind::Index);
        assert_eq!(ptr.is_leaf(), false);
        Self(unsafe { SortedPageRef::new(ptr) })
    }

    pub fn find(&self, target: &[u8]) -> Option<(&'a [u8], Index)> {
        self.0.seek_back(&target)
    }

    pub fn iter(&self) -> IndexPageIter<'a> {
        IndexPageIter::new(self.clone())
    }
}

impl<'a> Deref for IndexPageRef<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a> From<PagePtr> for IndexPageRef<'a> {
    fn from(ptr: PagePtr) -> Self {
        Self::new(ptr)
    }
}

/// An iterator over the entries of an index page.
pub struct IndexPageIter<'a>(SortedPageIter<'a, &'a [u8], Index>);

impl<'a> IndexPageIter<'a> {
    pub fn new(page: IndexPageRef<'a>) -> Self {
        Self(SortedPageIter::new(page.0))
    }
}

impl<'a> From<PagePtr> for IndexPageIter<'a> {
    fn from(ptr: PagePtr) -> Self {
        Self::new(ptr.into())
    }
}

impl<'a> From<IndexPageRef<'a>> for IndexPageIter<'a> {
    fn from(page: IndexPageRef<'a>) -> Self {
        Self::new(page)
    }
}

impl<'a> ForwardIter for IndexPageIter<'a> {
    type Key = &'a [u8];
    type Value = Index;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.0.last()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        self.0.next()
    }
}

impl<'a> SeekableIter for IndexPageIter<'a> {
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<Self::Key>,
    {
        self.0.seek(target);
    }
}

impl<'a> RewindableIter for IndexPageIter<'a> {
    fn rewind(&mut self) {
        self.0.rewind();
    }
}
