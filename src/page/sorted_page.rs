use super::{ForwardIter, PageBuf, PageBuilder, PageKind, PageTier};

pub(crate) struct SortedPageBuilder<'a, I> {
    base: PageBuilder,
    iter: Option<&'a mut I>,
}

impl<'a, I> SortedPageBuilder<'a, I>
where
    I: ForwardIter,
{
    pub(crate) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self {
            base: PageBuilder::new(tier, kind),
            iter: None,
        }
    }

    pub(crate) fn with_iter(mut self, iter: &'a mut I) -> Self {
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
