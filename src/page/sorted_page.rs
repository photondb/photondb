use super::{ForwardIter, PageBuf, PageBuilder, PageKind, PageTier};

pub(crate) struct SortedPageBuilder<'a, I, K, V>
where
    I: ForwardIter<Item = (K, V)>,
{
    base: PageBuilder,
    iter: Option<&'a mut I>,
}

impl<'a, I, K, V> SortedPageBuilder<'a, I, K, V>
where
    I: ForwardIter<Item = (K, V)>,
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
