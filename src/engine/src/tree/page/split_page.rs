use std::ops::Deref;

use super::*;

/// A builder to create split pages.
///
/// Note: We use the sorted page layout here to make it possible to split a page into multiple ones
/// in the future.
pub struct SplitPageBuilder(SortedPageBuilder);

impl Default for SplitPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::new(PageKind::Split))
    }
}

impl SplitPageBuilder {
    pub fn build_with_index<A>(
        self,
        alloc: &A,
        start: &[u8],
        index: Index,
    ) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        let mut iter = OptionIter::from((start, index));
        self.0.build_from_iter(alloc, &mut iter)
    }
}

/// An immutable reference to a split page.
#[derive(Clone)]
pub struct SplitPageRef<'a>(SortedPageRef<'a, &'a [u8], Index>);

impl<'a> SplitPageRef<'a> {
    pub fn new(base: PageRef<'a>) -> Self {
        debug_assert_eq!(base.kind(), PageKind::Split);
        Self(unsafe { SortedPageRef::new(base) })
    }

    pub fn split_index(&self) -> (&'a [u8], Index) {
        self.0.get_item(0).unwrap()
    }
}

impl<'a> Deref for SplitPageRef<'a> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a> From<PageRef<'a>> for SplitPageRef<'a> {
    fn from(base: PageRef<'a>) -> Self {
        Self::new(base)
    }
}

impl<'a> From<SplitPageRef<'a>> for PageRef<'a> {
    fn from(page: SplitPageRef<'a>) -> Self {
        page.0.into()
    }
}
