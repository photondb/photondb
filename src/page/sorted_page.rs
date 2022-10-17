use super::{Key, PageBuf, PageBuilder, PageKind, PageRef, PageTier, RewindableIter, SeekableIter};

pub(crate) struct SortedItem<'a> {
    pub(crate) key: Key<'a>,
    pub(crate) value: &'a [u8],
}

pub(crate) struct SortedPageBuilder<'a, I> {
    base: PageBuilder,
    iter: Option<&'a mut I>,
}

impl<'a, I> SortedPageBuilder<'a, I>
where
    I: RewindableIter<Item = SortedItem<'a>>,
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

pub(crate) struct SortedPageRef<'a> {
    page: PageRef<'a>,
}

impl<'a> SortedPageRef<'a> {
    pub(crate) fn new(page: PageRef<'a>) -> Self {
        Self { page }
    }

    pub(crate) fn len(&self) -> usize {
        todo!()
    }

    pub(crate) fn get(&self, index: usize) -> Option<SortedItem<'a>> {
        todo!()
    }

    pub(crate) fn rank(&self) -> usize {
        todo!()
    }
}

impl<'a, T> From<T> for SortedPageRef<'a>
where
    T: Into<PageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

pub(crate) struct SortedPageIter<'a> {
    page: SortedPageRef<'a>,
    next: usize,
}

impl<'a> SortedPageIter<'a> {
    pub(crate) fn new(page: SortedPageRef<'a>) -> Self {
        Self { page, next: 0 }
    }
}

impl<'a, T> From<T> for SortedPageIter<'a>
where
    T: Into<SortedPageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

impl<'a> Iterator for SortedPageIter<'a> {
    type Item = SortedItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.page.get(self.next) {
            self.next += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<'a> SeekableIter for SortedPageIter<'a> {
    fn seek(&mut self) {
        self.next = self.page.rank();
    }
}

impl<'a> RewindableIter for SortedPageIter<'a> {
    fn rewind(&mut self) {
        self.next = 0;
    }
}
