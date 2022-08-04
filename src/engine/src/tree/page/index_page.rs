use std::ops::Deref;

use super::*;

pub type IndexItem<'a> = (&'a [u8], Index);

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
        I: RewindableIter<Item = IndexItem<'a>>,
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
        assert!(!base.is_data());
        Self(unsafe { SortedPageRef::new(base) })
    }

    /// Returns the entry that contains `target`.
    pub fn find(&self, target: &[u8]) -> Option<IndexItem<'a>> {
        self.0.seek_back(&target)
    }

    /// Returns the two entries that enclose `target`.
    pub fn find_range(&self, target: &[u8]) -> (Option<IndexItem<'a>>, Option<IndexItem<'a>>) {
        match self.0.search(&target) {
            Ok(i) => (self.0.get(i), i.checked_add(1).and_then(|i| self.0.get(i))),
            Err(i) => (i.checked_sub(1).and_then(|i| self.0.get(i)), self.0.get(i)),
        }
    }

    /// Creates an iterator over entries in this page.
    pub fn iter(&self) -> IndexPageIter<'a> {
        IndexPageIter::new(self.clone())
    }

    /// Returns a split key and an iterator over the entries at or after the split key.
    pub fn split(&self) -> Option<(&'a [u8], BoundedIter<IndexPageIter<'a>>)> {
        if let Some((sep, _)) = self.0.get(self.0.len() / 2) {
            let rank = match self.0.search(&sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if rank > 0 {
                let iter = BoundedIter::new(self.iter(), rank);
                return Some((sep, iter));
            }
        }
        None
    }
}

impl<'a> Deref for IndexPageRef<'a> {
    type Target = SortedPageRef<'a, &'a [u8], Index>;

    fn deref(&self) -> &Self::Target {
        &self.0
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
    type Item = IndexItem<'a>;

    fn last(&self) -> Option<&Self::Item> {
        self.0.last()
    }

    fn next(&mut self) -> Option<&Self::Item> {
        self.0.next()
    }

    fn skip(&mut self, n: usize) {
        self.0.skip(n);
    }

    fn skip_all(&mut self) {
        self.0.skip_all();
    }
}

impl<'a> SeekableIter<[u8]> for IndexPageIter<'a> {
    fn seek(&mut self, target: &[u8]) {
        self.0.seek(target);
    }
}

impl<'a> RewindableIter for IndexPageIter<'a> {
    fn rewind(&mut self) {
        self.0.rewind();
    }
}
