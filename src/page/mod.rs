mod iter;
pub(crate) use iter::{
    ItemIter, MergingIter, MergingIterBuilder, RewindableIterator, SeekableIterator, SliceIter,
};

mod data;
pub(crate) use data::{Index, Key, Range, Value};

mod codec;

mod base_page;
use base_page::PageBuilder;
pub(crate) use base_page::{PageBuf, PageKind, PageRef, PageTier};

mod sorted_page;
pub(crate) use sorted_page::{
    SortedPageBuilder, SortedPageIter, SortedPageKey, SortedPageRef, SortedPageValue,
};

pub(crate) type ValuePageRef<'a> = SortedPageRef<'a, Key<'a>, Value<'a>>;
pub(crate) type IndexPageRef<'a> = SortedPageRef<'a, &'a [u8], Index>;
