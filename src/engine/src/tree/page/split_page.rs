use std::ops::Deref;

use super::*;

/// A builder to create split pages.
pub struct SplitPageBuilder(SortedPageBuilder);

impl SplitPageBuilder {
    pub fn new(is_leaf: bool) -> Self {
        Self(SortedPageBuilder::new(PageKind::Split, is_leaf))
    }

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
    pub fn new(ptr: PagePtr) -> Self {
        assert_eq!(ptr.kind(), PageKind::Split);
        Self(unsafe { SortedPageRef::new(ptr) })
    }

    pub fn get(&self) -> (&'a [u8], Index) {
        self.0.get(0).unwrap()
    }
}

impl<'a> Deref for SplitPageRef<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a> From<PagePtr> for SplitPageRef<'a> {
    fn from(ptr: PagePtr) -> Self {
        Self::new(ptr)
    }
}
