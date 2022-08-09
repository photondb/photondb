use super::*;

/// A builder to create split pages.
pub struct SplitPageBuilder(DeltaPageBuilder);

impl Default for SplitPageBuilder {
    fn default() -> Self {
        Self(DeltaPageBuilder::new(PageKind::Split, true))
    }
}

impl SplitPageBuilder {
    pub fn build<A>(self, alloc: &A, item: &IndexItem<'_>) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        self.0.build(alloc, item)
    }
}

pub type SplitPageRef<'a> = DeltaPageRef<'a, IndexItem<'a>>;
