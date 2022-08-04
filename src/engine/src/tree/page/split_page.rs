use std::ops::Deref;

use super::*;

/// A builder to create split pages.
pub struct SplitPageBuilder(SortedPageBuilder);

impl Default for SplitPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::new(PageKind::Split, true))
    }
}

impl SplitPageBuilder {
    pub fn build_with_index<A>(
        self,
        alloc: &A,
        key: &[u8],
        index: Index,
    ) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        let mut iter = OptionIter::from((key, index));
        self.0.build_from_iter(alloc, &mut iter)
    }
}

/// An immutable reference to a split page.
#[derive(Clone)]
pub struct SplitPageRef<'a>(SortedPageRef<'a, &'a [u8], Index>);

impl<'a> SplitPageRef<'a> {
    pub fn new(base: PageRef<'a>) -> Self {
        assert_eq!(base.kind(), PageKind::Split);
        Self(unsafe { SortedPageRef::new(base) })
    }

    pub fn get(&self) -> (&'a [u8], Index) {
        self.0.get(0).unwrap()
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
