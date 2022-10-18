use std::marker::PhantomData;

use super::{
    codec::{DecodeFrom, EncodeTo},
    Key, PageBuf, PageBuilder, PageKind, PageRef, PageTier, RewindableIterator, SeekableIterator,
};

type SortedItem<'a, V> = (Key<'a>, V);

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

    pub(crate) fn with_iter(mut self, iter: I) -> Self {
        self.iter = Some(iter);
        self
    }

    pub(crate) fn size(&self) -> usize {
        todo!()
    }

    pub(crate) fn build(&self, page: &mut PageBuf<'_>) {
        todo!()
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

    pub(crate) fn rank(&self, target: &Key<'_>) -> usize {
        todo!()
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
        self.next = self.page.rank(target);
    }
}

impl<'a, V> RewindableIterator for SortedPageIter<'a, V> {
    fn rewind(&mut self) {
        self.next = 0;
    }
}
