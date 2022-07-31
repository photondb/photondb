use std::ops::{Deref, DerefMut};

use super::*;

/// A builder to create data pages.
pub struct DataPageBuilder(SortedPageBuilder);

impl Default for DataPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::new(PageKind::Delta, false))
    }
}

impl DataPageBuilder {
    /// Builds an empty data page.
    pub fn build<A>(self, alloc: &A) -> Result<DataPageBuf, A::Error>
    where
        A: PageAlloc,
    {
        self.0.build(alloc).map(DataPageBuf)
    }

    /// Builds a data page with entries from the given iterator.
    pub fn build_from_iter<'a, A, I>(
        mut self,
        alloc: &A,
        iter: &mut I,
    ) -> Result<DataPageBuf, A::Error>
    where
        A: PageAlloc,
        I: RewindableIter<Key = Key<'a>, Value = Value<'a>>,
    {
        self.0.build_from_iter(alloc, iter).map(DataPageBuf)
    }
}

pub struct DataPageBuf(SortedPageBuf);

impl DataPageBuf {
    pub fn as_ptr(&mut self) -> PagePtr {
        self.0.as_ptr()
    }

    pub fn as_ref<'a>(&self) -> DataPageRef<'a> {
        DataPageRef(self.0.as_ref())
    }
}

impl Deref for DataPageBuf {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl DerefMut for DataPageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

/// An immutable reference to a data page.
#[derive(Clone)]
pub struct DataPageRef<'a>(SortedPageRef<'a, Key<'a>, Value<'a>>);

impl<'a> DataPageRef<'a> {
    pub fn new(ptr: PagePtr) -> Self {
        assert_eq!(ptr.kind(), PageKind::Delta);
        assert_eq!(ptr.is_index(), false);
        Self(unsafe { SortedPageRef::new(ptr) })
    }

    pub fn find(&self, target: &Key<'_>) -> Option<(Key<'a>, Value<'a>)> {
        if let Some((k, v)) = self.0.seek(target) {
            if k.raw == target.raw {
                return Some((k, v));
            }
        }
        None
    }
}

impl<'a> From<PagePtr> for DataPageRef<'a> {
    fn from(ptr: PagePtr) -> Self {
        Self::new(ptr)
    }
}

impl<'a> DeltaPageRef for DataPageRef<'a> {
    type Key = Key<'a>;
    type Value = Value<'a>;
    type PageIter = DataPageIter<'a>;

    fn iter(&self) -> Self::PageIter {
        DataPageIter::new(self.clone())
    }
}

/// An iterator over the entries of a data page.
pub struct DataPageIter<'a>(SortedPageIter<'a, Key<'a>, Value<'a>>);

impl<'a> DataPageIter<'a> {
    pub fn new(page: DataPageRef<'a>) -> Self {
        Self(SortedPageIter::new(page.0))
    }
}

impl<'a> From<PagePtr> for DataPageIter<'a> {
    fn from(ptr: PagePtr) -> Self {
        Self::new(ptr.into())
    }
}

impl<'a> From<DataPageRef<'a>> for DataPageIter<'a> {
    fn from(page: DataPageRef<'a>) -> Self {
        Self::new(page)
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
