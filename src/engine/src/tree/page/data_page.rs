use std::ops::Deref;

use super::*;

/// A builder to create data pages.
pub struct DataPageBuilder(SortedPageBuilder);

impl Default for DataPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::with_leaf(PageKind::Data, true))
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

    /// Builds a data page with items from the given iterator.
    pub fn build_from_iter<'a, A, I>(self, alloc: &A, iter: &mut I) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
        I: ForwardIter<Item = DataItem<'a>>,
    {
        self.0.build_from_iter(alloc, iter)
    }
}

/// An immutable reference to a data page.
#[derive(Clone)]
pub struct DataPageRef<'a>(SortedPageRef<'a, Key<'a>, Value<'a>>);

impl<'a> DataPageRef<'a> {
    pub fn new(base: PageRef<'a>) -> Self {
        debug_assert_eq!(base.kind(), PageKind::Data);
        debug_assert_eq!(base.is_leaf(), true);
        Self(unsafe { SortedPageRef::new(base) })
    }

    /// Returns the item that matches `target`.
    pub fn find(&self, target: Key<'_>) -> Option<DataItem<'a>> {
        if let Some((k, v)) = self.0.seek_item(&target) {
            if k.raw == target.raw {
                return Some((k, v));
            }
        }
        None
    }

    /// Creates an iterator over items of this page.
    pub fn iter(&self) -> DataPageIter<'a> {
        DataPageIter::new(self.0.clone())
    }

    /// Returns a split key for split and an iterator over items at and after the split key.
    pub fn split(&self) -> Option<(&'a [u8], BoundedIter<DataPageIter<'a>>)> {
        if let Some((mut sep, _)) = self.0.get_item(self.0.item_len() / 2) {
            // Avoids splitting items of the same raw key.
            sep.lsn = u64::MAX;
            let index = match self.0.rank_item(&sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if index > 0 {
                let iter = BoundedIter::new(self.iter(), index);
                return Some((sep.raw, iter));
            }
        }
        None
    }
}

impl<'a> Deref for DataPageRef<'a> {
    type Target = SortedPageRef<'a, Key<'a>, Value<'a>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> From<PageRef<'a>> for DataPageRef<'a> {
    fn from(base: PageRef<'a>) -> Self {
        Self::new(base)
    }
}

impl<'a> From<DataPageRef<'a>> for PageRef<'a> {
    fn from(page: DataPageRef<'a>) -> Self {
        page.0.into()
    }
}

impl<'a> From<DataPageRef<'a>> for SortedPageRef<'a, Key<'a>, Value<'a>> {
    fn from(page: DataPageRef<'a>) -> Self {
        page.0
    }
}

pub type DataPageIter<'a> = SortedPageIter<'a, Key<'a>, Value<'a>>;
