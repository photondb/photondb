use std::ops::{Deref, Range};

use super::*;

pub struct SplitPageBuilder {
    base: PageBuilder,
    size: usize,
}

impl SplitPageBuilder {
    pub fn new(is_leaf: bool) -> Self {
        Self {
            base: PageBuilder::new(PageKind::Split, is_leaf),
            size: 0,
        }
    }

    fn add(&mut self, range: Range<&[u8]>, index: Index) {
        self.size += range.encode_size() + index.encode_size();
    }

    pub fn build_with_index<A>(
        mut self,
        alloc: &A,
        range: Range<&[u8]>,
        index: Index,
    ) -> Option<PagePtr>
    where
        A: PageAlloc,
    {
        self.add(range.clone(), index);
        let ptr = self.base.build(alloc, self.size);
        ptr.map(|ptr| unsafe {
            let mut buf = SplitPageBuf::new(ptr, self);
            buf.add(range, index);
            ptr
        })
    }
}

struct SplitPageBuf {
    content: BufWriter,
}

impl SplitPageBuf {
    unsafe fn new(mut ptr: PagePtr, builder: SplitPageBuilder) -> Self {
        Self {
            content: BufWriter::new(ptr.content_mut()),
        }
    }

    unsafe fn add(&mut self, range: Range<&[u8]>, index: Index) {
        range.encode_to(&mut self.content);
        index.encode_to(&mut self.content);
    }
}

pub struct SplitPageRef<'a> {
    base: PageRef<'a>,
    range: Range<&'a [u8]>,
    index: Index,
}

impl<'a> SplitPageRef<'a> {
    pub unsafe fn new(base: PageRef<'a>) -> Self {
        let mut content = BufReader::new(base.content());
        let range = Range::decode_from(&mut content);
        let index = Index::decode_from(&mut content);
        Self { base, range, index }
    }

    pub fn range(&self) -> Range<&'a [u8]> {
        self.range.clone()
    }

    pub fn index(&self) -> Index {
        self.index
    }
}

impl<'a> Deref for SplitPageRef<'a> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'a> From<SplitPageRef<'a>> for PageRef<'a> {
    fn from(page: SplitPageRef<'a>) -> Self {
        page.base
    }
}
