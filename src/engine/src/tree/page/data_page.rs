use std::ops::Deref;

use super::*;

/// A builder to create data pages.
pub struct DataPageBuilder(SortedPageBuilder);

impl Default for DataPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::new(PageKind::Data, true))
    }
}

impl DataPageBuilder {
    /// Builds an empty data page.
    pub fn build<A>(self, alloc: &A) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        self.0.build(alloc)
    }

    /// Builds a data page with entries from the given iterator.
    pub fn build_from_iter<'a, A, I>(self, alloc: &A, iter: &mut I) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
        I: RewindableIter<Key = Key<'a>, Value = Value<'a>>,
    {
        self.0.build_from_iter(alloc, iter)
    }
}

/// An immutable reference to a data page.
#[derive(Clone)]
pub struct DataPageRef<'a>(SortedPageRef<'a, Key<'a>, Value<'a>>);

impl<'a> DataPageRef<'a> {
    pub fn new(base: PageRef<'a>) -> Self {
        assert_eq!(base.kind(), PageKind::Data);
        assert_eq!(base.is_leaf(), true);
        Self(unsafe { SortedPageRef::new(base) })
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn find(&self, target: Key<'_>) -> Option<(Key<'a>, Value<'a>)> {
        if let Some((k, v)) = self.0.seek(&target) {
            if k.raw == target.raw {
                return Some((k, v));
            }
        }
        None
    }

    pub fn iter(&self) -> DataPageIter<'a> {
        DataPageIter::new(self.clone())
    }

    pub fn split(&self) -> Option<(Key<'a>, DataPageSplitIter<'a>)> {
        if let Some((k, _)) = self.0.get(self.0.len() / 2) {
            let sep = Key::new(k.raw, u64::MAX);
            let rank = match self.0.search(&sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if rank > 0 {
                let iter = DataPageSplitIter::new(self.iter(), rank);
                return Some((sep, iter));
            }
        }
        None
    }
}

impl<'a> Deref for DataPageRef<'a> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a, T> From<T> for DataPageRef<'a>
where
    T: Into<PageRef<'a>>,
{
    fn from(base: T) -> Self {
        Self::new(base.into())
    }
}

/// An iterator over the entries of a data page.
pub struct DataPageIter<'a>(SortedPageIter<'a, Key<'a>, Value<'a>>);

impl<'a> DataPageIter<'a> {
    pub fn new(page: DataPageRef<'a>) -> Self {
        Self(SortedPageIter::new(page.0))
    }

    pub fn skip(&mut self, n: usize) {
        self.0.skip(n)
    }
}

impl<'a, T> From<T> for DataPageIter<'a>
where
    T: Into<DataPageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

impl<'a> ForwardIter for DataPageIter<'a> {
    type Key = Key<'a>;
    type Value = Value<'a>;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.0.last()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        self.0.next()
    }
}

impl<'a> SeekableIter for DataPageIter<'a> {
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<Self::Key>,
    {
        self.0.seek(target);
    }
}

impl<'a> RewindableIter for DataPageIter<'a> {
    fn rewind(&mut self) {
        self.0.rewind();
    }
}

pub struct DataPageSplitIter<'a> {
    base: DataPageIter<'a>,
    skip: usize,
}

impl<'a> DataPageSplitIter<'a> {
    fn new(mut base: DataPageIter<'a>, skip: usize) -> Self {
        base.skip(skip);
        Self { base, skip }
    }
}

impl<'a> ForwardIter for DataPageSplitIter<'a> {
    type Key = Key<'a>;
    type Value = Value<'a>;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.base.last()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        self.base.next()
    }
}

impl<'a> RewindableIter for DataPageSplitIter<'a> {
    fn rewind(&mut self) {
        self.base.rewind();
        self.base.skip(self.skip);
    }
}
