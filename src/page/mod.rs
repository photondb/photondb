mod data;
pub(crate) use data::{Index, Key, Range, Value};

mod iter;
pub(crate) use iter::{
    ItemIter, MergingIter, MergingIterBuilder, RewindableIterator, SeekableIterator, SliceIter,
};

mod base_page;
pub(crate) use base_page::{PageBuf, PageBuilder, PageKind, PagePtr, PageRef, PageTier};

mod sorted_page;
pub(crate) use sorted_page::{SortedItem, SortedPageBuilder, SortedPageIter, SortedPageRef};

pub(crate) type DataPageRef<'a> = SortedPageRef<'a, &'a [u8]>;
pub(crate) type SplitPageRef<'a> = SortedPageRef<'a, Index>;
pub(crate) type LeafDataPageIter<'a> = SortedPageIter<'a, Value<'a>>;
pub(crate) type InnerDataPageIter<'a> = SortedPageIter<'a, Index>;
