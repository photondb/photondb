use std::{cmp::Ordering, marker::PhantomData};

use super::*;
use crate::util::{BufReader, BufWriter};

pub struct DeltaPageBuilder {
    base: PageBuilder,
}

impl DeltaPageBuilder {
    pub fn new(kind: PageKind, is_data: bool) -> Self {
        Self {
            base: PageBuilder::new(kind, is_data),
        }
    }

    pub fn build<A, I>(self, alloc: &A, item: &I) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
        I: EncodeTo,
    {
        let ptr = self.base.build(alloc, item.encode_size());
        ptr.map(|mut ptr| unsafe {
            let mut buf = BufWriter::new(ptr.content_mut());
            item.encode_to(&mut buf);
            ptr
        })
    }
}

pub struct DeltaPageRef<'a, I> {
    base: PageRef<'a>,
    _mark: PhantomData<I>,
}

impl<'a, I> DeltaPageRef<'a, I>
where
    I: DecodeFrom,
{
    pub fn new(base: PageRef<'a>) -> Self {
        Self {
            base,
            _mark: PhantomData,
        }
    }

    pub fn item(&self) -> I {
        let mut buf = BufReader::new(self.base.content());
        unsafe { I::decode_from(&mut buf) }
    }
}

impl<'a, I> From<PageRef<'a>> for DeltaPageRef<'a, I>
where
    I: DecodeFrom,
{
    fn from(base: PageRef<'a>) -> Self {
        Self::new(base)
    }
}

impl<'a, I> From<DeltaPageRef<'a, I>> for PageRef<'a>
where
    I: DecodeFrom,
{
    fn from(page: DeltaPageRef<'a, I>) -> Self {
        page.base
    }
}

pub struct DeltaPageIter<'a, I> {
    page: DeltaPageRef<'a, I>,
    current: Option<I>,
}

impl<'a, I> ForwardIter for DeltaPageIter<'a, I>
where
    I: DecodeFrom + Ord,
{
    type Item = I;

    fn current(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }

    fn rewind(&mut self) {
        self.current = Some(self.page.item());
    }

    fn next(&mut self) {
        self.current.take();
    }

    fn skip_all(&mut self) {
        self.current.take();
    }
}

impl<'a, I, T> SeekableIter<T> for DeltaPageIter<'a, I>
where
    I: DecodeFrom + Ord,
    T: Compare<I> + ?Sized,
{
    fn seek(&mut self, target: &T) {
        let item = self.page.item();
        if target.compare(&item) != Ordering::Greater {
            self.current = Some(item);
        } else {
            self.current = None;
        }
    }
}
