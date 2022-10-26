use std::{marker::PhantomData, ops::Deref};

use super::{
    PageBuf, PageBuilder, PageKind, PageRef, PageTier, RewindableIterator, SeekableIterator,
};
use crate::util::codec::{DecodeFrom, EncodeTo};

pub(crate) struct SortedPageBuilder<I> {
    base: PageBuilder,
    iter: Option<I>,
}

impl<'a, I, K, V> SortedPageBuilder<I>
where
    I: RewindableIterator<Item = (K, V)>,
    K: EncodeTo + DecodeFrom,
    V: EncodeTo + DecodeFrom,
{
    pub(crate) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self {
            base: PageBuilder::new(tier, kind),
            iter: None,
        }
    }

    pub(crate) fn with_iter(mut self, iter: I) -> Self {
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

pub(crate) struct SortedPageRef<'a, K, V> {
    page: PageRef<'a>,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> SortedPageRef<'a, K, V> {
    pub(crate) fn new(page: PageRef<'a>) -> Self {
        Self {
            page,
            _marker: PhantomData,
        }
    }

    pub(crate) fn len(&self) -> usize {
        todo!()
    }

    pub(crate) fn get(&self, index: usize) -> Option<(K, V)> {
        todo!()
    }

    pub(crate) fn rank(&self, target: &K) -> Result<usize, usize> {
        todo!()
    }

    pub(crate) fn split(&self) -> Option<(K, SortedPageIter<'a, K, V>)> {
        todo!()
    }
}

impl<'a, K, V> Deref for SortedPageRef<'a, K, V> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl<'a, K, V, T> From<T> for SortedPageRef<'a, K, V>
where
    T: Into<PageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

pub(crate) struct SortedPageIter<'a, K, V> {
    page: SortedPageRef<'a, K, V>,
    next: usize,
}

impl<'a, K, V> SortedPageIter<'a, K, V> {
    pub(crate) fn new(page: SortedPageRef<'a, K, V>) -> Self {
        Self { page, next: 0 }
    }
}

impl<'a, K, V, T> From<T> for SortedPageIter<'a, K, V>
where
    T: Into<SortedPageRef<'a, K, V>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

impl<'a, K, V> Iterator for SortedPageIter<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.page.get(self.next) {
            self.next += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<'a, K, V> SeekableIterator<K> for SortedPageIter<'a, K, V> {
    fn seek(&mut self, target: &K) {
        todo!()
    }
}

impl<'a, K, V> RewindableIterator for SortedPageIter<'a, K, V> {
    fn rewind(&mut self) {
        self.next = 0;
    }
}
