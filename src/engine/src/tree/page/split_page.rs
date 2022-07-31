use std::ops::{Deref, DerefMut, Range};

use super::*;

/// A builder to create split pages.
pub struct SplitPageBuilder {
    base: PageBuilder,
    size: usize,
}

impl Default for SplitPageBuilder {
    fn default() -> Self {
        Self::with_index(false)
    }
}

impl SplitPageBuilder {
    pub fn with_index(is_index: bool) -> Self {
        Self {
            base: PageBuilder::new(PageKind::Split, is_index),
            size: 0,
        }
    }

    fn add(&mut self, range: Range<&[u8]>, index: Index) {
        self.size += range.encode_size() + index.encode_size();
    }

    /// Builds a split page with the given index.
    pub fn build_with_index<A>(
        mut self,
        alloc: &A,
        range: Range<&[u8]>,
        index: Index,
    ) -> Result<SplitPageBuf, A::Error>
    where
        A: PageAlloc,
    {
        self.add(range.clone(), index);
        let ptr = self.base.build(alloc, self.size);
        ptr.map(|ptr| unsafe {
            let mut buf = SplitPageBuf::new(ptr);
            buf.add(range, index);
            buf
        })
    }
}

pub struct SplitPageBuf {
    ptr: PagePtr,
    content: BufWriter,
}

impl SplitPageBuf {
    unsafe fn new(mut ptr: PagePtr) -> Self {
        Self {
            ptr,
            content: BufWriter::new(ptr.content_mut()),
        }
    }

    unsafe fn add(&mut self, range: Range<&[u8]>, index: Index) {
        range.encode_to(&mut self.content);
        index.encode_to(&mut self.content);
    }

    pub fn as_ptr(self) -> PagePtr {
        self.ptr
    }

    pub fn as_ref<'a>(self) -> SplitPageRef<'a> {
        SplitPageRef::new(self.ptr)
    }
}

impl Deref for SplitPageBuf {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl DerefMut for SplitPageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ptr
    }
}

/// An immutable reference to a split page.
pub struct SplitPageRef<'a> {
    ptr: PagePtr,
    range: Range<&'a [u8]>,
    index: Index,
}

impl<'a> SplitPageRef<'a> {
    pub fn new(ptr: PagePtr) -> Self {
        assert_eq!(ptr.kind(), PageKind::Split);
        unsafe {
            let mut content = BufReader::new(ptr.content());
            let range = Range::decode_from(&mut content);
            let index = Index::decode_from(&mut content);
            Self { ptr, range, index }
        }
    }

    pub fn range(&self) -> Range<&'a [u8]> {
        self.range.clone()
    }

    pub fn index(&self) -> Index {
        self.index
    }
}

impl<'a> Deref for SplitPageRef<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<'a> From<PagePtr> for SplitPageRef<'a> {
    fn from(ptr: PagePtr) -> Self {
        Self::new(ptr)
    }
}
