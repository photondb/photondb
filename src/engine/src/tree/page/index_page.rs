use std::{cmp::Ordering, ops::Deref};

use super::*;

pub type IndexItem<'a> = (&'a [u8], Index);

impl<'a> Compare<IndexItem<'a>> for IndexItem<'a> {
    fn compare(&self, other: &Self) -> Ordering {
        self.0.cmp(other.0)
    }
}

/// A builder to create index pages.
pub struct IndexPageBuilder(SortedPageBuilder);

impl Default for IndexPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::new(PageKind::Index, false))
    }
}

impl IndexPageBuilder {
    /// Builds an index page with items from the given iterator.
    pub fn build_from_iter<'a, A, I>(self, alloc: &A, iter: &mut I) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
        I: ForwardIter<Item = IndexItem<'a>>,
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
        Self(unsafe { SortedPageRef::new(base) })
    }

    /// Returns the two items that enclose `target`.
    pub fn find(&self, target: &[u8]) -> (Option<IndexItem<'a>>, Option<IndexItem<'a>>) {
        match self.0.rank_item(&target) {
            Ok(i) => (
                self.0.get_item(i),
                i.checked_add(1).and_then(|i| self.0.get_item(i)),
            ),
            Err(i) => (
                i.checked_sub(1).and_then(|i| self.0.get_item(i)),
                self.0.get_item(i),
            ),
        }
    }

    /// Creates an iterator over items of this page.
    pub fn iter(&self) -> IndexPageIter<'a> {
        IndexPageIter::new(self.clone())
    }

    /// Returns a split key and an iterator over items at or after the split key.
    pub fn split(&self) -> Option<(&'a [u8], BoundedIter<IndexPageIter<'a>>)> {
        if let Some((sep, _)) = self.0.get_item(self.0.num_items() / 2) {
            let index = match self.0.rank_item(&sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if index > 0 {
                let iter = BoundedIter::new(self.iter(), index);
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

impl<'a> From<PageRef<'a>> for IndexPageRef<'a> {
    fn from(base: PageRef<'a>) -> Self {
        Self::new(base)
    }
}

/// An iterator over items of an index page.
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

    #[inline]
    fn current(&self) -> Option<&Self::Item> {
        self.0.current()
    }

    #[inline]
    fn rewind(&mut self) {
        self.0.rewind();
    }

    #[inline]
    fn next(&mut self) {
        self.0.next()
    }

    #[inline]
    fn skip(&mut self, n: usize) {
        self.0.skip(n);
    }

    #[inline]
    fn skip_all(&mut self) {
        self.0.skip_all();
    }
}

impl<'a> SeekableIter<[u8]> for IndexPageIter<'a> {
    #[inline]
    fn seek(&mut self, target: &[u8]) {
        self.0.seek(target);
    }
}
