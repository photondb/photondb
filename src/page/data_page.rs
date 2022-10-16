use super::{PageBuf, PageKind, PageTier, SortedPageBuilder};

pub(crate) struct DataPageBuilder(SortedPageBuilder);

impl DataPageBuilder {
    pub(crate) fn new(tier: PageTier) -> Self {
        Self(SortedPageBuilder::new(tier, PageKind::Data))
    }

    pub(crate) fn size(&self) -> usize {
        todo!()
    }

    pub(crate) fn with_item(mut self, item: ()) -> Self {
        todo!()
    }

    pub(crate) fn with_iter(mut self, iter: ()) -> Self {
        todo!()
    }

    pub(crate) fn build(&self, page: &mut PageBuf<'_>) {
        todo!()
    }
}
