use std::{cmp::Ordering, marker::PhantomData, ops::Deref};

use super::{
    Key, PageBuf, PageBuilder, PageKind, PageRef, PageTier, RewindableIterator, SeekableIterator,
};
use crate::util::codec::{DecodeFrom, EncodeTo};

pub(crate) struct SortedItem<'a, V>(pub(crate) Key<'a>, pub(crate) V);

impl<'a, V> Eq for SortedItem<'a, V> {}

impl<'a, V> PartialEq for SortedItem<'a, V> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<'a, V> Ord for SortedItem<'a, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl<'a, V> PartialOrd for SortedItem<'a, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, V: Clone> Clone for SortedItem<'a, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<'a, V> From<(Key<'a>, V)> for SortedItem<'a, V> {
    fn from((k, v): (Key<'a>, V)) -> Self {
        Self(k, v)
    }
}

impl<'a, V> From<SortedItem<'a, V>> for (Key<'a>, V) {
    fn from(item: SortedItem<'a, V>) -> Self {
        (item.0, item.1)
    }
}

pub(crate) struct SortedPageBuilder<'a, I, V>
where
    I: RewindableIterator<Item = SortedItem<'a, V>>,
{
    base: PageBuilder,
    iter: Option<I>,
}

impl<'a, I, V> SortedPageBuilder<'a, I, V>
where
    I: RewindableIterator<Item = SortedItem<'a, V>>,
    V: EncodeTo + DecodeFrom,
{
    pub(crate) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self {
            base: PageBuilder::new(tier, kind),
            iter: None,
        }
    }

    pub(crate) fn with_iter(mut self, mut iter: I) -> Self {
        self.iter = Some(iter);
        self
    }

    pub(crate) fn size(&self) -> usize {
        todo!()
    }

    pub(crate) fn build(&self, page: &mut PageBuf<'_>) {
        self.base.build(page);
    }
}

pub(crate) struct SortedPageRef<'a, V> {
    page: PageRef<'a>,
    _marker: PhantomData<V>,
}

impl<'a, V> SortedPageRef<'a, V> {
    pub(crate) fn new(page: PageRef<'a>) -> Self {
        Self {
            page,
            _marker: PhantomData,
        }
    }

    pub(crate) fn len(&self) -> usize {
        todo!()
    }

    pub(crate) fn get(&self, index: usize) -> Option<SortedItem<'a, V>> {
        todo!()
    }

    pub(crate) fn rank(&self, target: &Key<'_>) -> Result<usize, usize> {
        todo!()
    }

    pub(crate) fn split(&self) -> Option<(Key<'_>, SortedPageIter<'a, V>)> {
        todo!()
    }
}

impl<'a, V> Deref for SortedPageRef<'a, V> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl<'a, V, T> From<T> for SortedPageRef<'a, V>
where
    T: Into<PageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

pub(crate) struct SortedPageIter<'a, V> {
    page: SortedPageRef<'a, V>,
    next: usize,
}

impl<'a, V> SortedPageIter<'a, V> {
    pub(crate) fn new(page: SortedPageRef<'a, V>) -> Self {
        Self { page, next: 0 }
    }
}

impl<'a, V, T> From<T> for SortedPageIter<'a, V>
where
    T: Into<SortedPageRef<'a, V>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

impl<'a, V> Iterator for SortedPageIter<'a, V> {
    type Item = SortedItem<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.page.get(self.next) {
            self.next += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<'a, V> SeekableIterator<Key<'_>> for SortedPageIter<'a, V> {
    fn seek(&mut self, target: &Key<'_>) {
        todo!()
    }
}

impl<'a, V> RewindableIterator for SortedPageIter<'a, V> {
    fn rewind(&mut self) {
        self.next = 0;
    }
}
