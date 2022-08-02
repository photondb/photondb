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
    pub fn new(base: PageRef<'a>) -> Self {
        assert_eq!(base.kind(), PageKind::Index);
        assert_eq!(base.is_leaf(), false);
        Self(unsafe { SortedPageRef::new(base) })
    }

    /// Returns the entry that covers `target`.
    pub fn find(&self, target: &[u8]) -> Option<(&'a [u8], Index)> {
        self.0.seek_back(&target)
    }

    /// Returns the two entries that enclose `target`.
    pub fn find_range(
        &self,
        target: &[u8],
    ) -> (Option<(&'a [u8], Index)>, Option<(&'a [u8], Index)>) {
        match self.0.search(&target) {
            Ok(i) => (self.0.get(i), self.0.get(i + 1)),
            Err(i) => {
                if i > 0 {
                    (self.0.get(i - 1), self.0.get(i))
                } else {
                    (None, self.0.get(i))
                }
            }
        }
    }

    pub fn iter(&self) -> IndexPageIter<'a> {
        IndexPageIter::new(self.clone())
    }

    pub fn split(&self) -> Option<(&'a [u8], IndexPageSplitIter<'a>)> {
        if let Some((sep, _)) = self.0.get(self.0.len() / 2) {
            let rank = match self.0.search(&sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if rank > 0 {
                let iter = IndexPageSplitIter::new(self.iter(), rank);
                return Some((sep, iter));
            }
        }
        None
    }
}

impl<'a> Deref for IndexPageRef<'a> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a, T> From<T> for IndexPageRef<'a>
where
    T: Into<PageRef<'a>>,
{
    fn from(base: T) -> Self {
        Self::new(base.into())
    }
}

/// An iterator over the entries of an index page.
pub struct IndexPageIter<'a>(SortedPageIter<'a, &'a [u8], Index>);

impl<'a> IndexPageIter<'a> {
    pub fn new(page: IndexPageRef<'a>) -> Self {
        Self(SortedPageIter::new(page.0))
    }

    pub fn skip(&mut self, n: usize) {
        self.0.skip(n)
    }
}

impl<'a, T> From<T> for IndexPageIter<'a>
where
    T: Into<IndexPageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
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

pub struct IndexPageSplitIter<'a> {
    base: IndexPageIter<'a>,
    skip: usize,
}

impl<'a> IndexPageSplitIter<'a> {
    fn new(mut base: IndexPageIter<'a>, skip: usize) -> Self {
        base.skip(skip);
        Self { base, skip }
    }
}

impl<'a> ForwardIter for IndexPageSplitIter<'a> {
    type Key = &'a [u8];
    type Value = Index;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.base.last()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        self.base.next()
    }
}

impl<'a> RewindableIter for IndexPageSplitIter<'a> {
    fn rewind(&mut self) {
        self.base.rewind();
        self.base.skip(self.skip);
    }
}
