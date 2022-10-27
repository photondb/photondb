mod iter;
pub(crate) use iter::{
    ItemIter, MergingIter, MergingIterBuilder, RewindableIterator, SeekableIterator, SliceIter,
};

mod data;
pub(crate) use data::{Index, Key, Range, Value};

mod codec;
pub(crate) use codec::{DecodeFrom, EncodeTo};

mod base_page;
pub(crate) use base_page::{PageBuf, PageBuilder, PageKind, PageRef, PageTier};

mod sorted_page;
pub(crate) use sorted_page::{SortedPageBuilder, SortedPageIter, SortedPageRef};

pub(crate) type ValuePageRef<'a> = SortedPageRef<'a, Key<'a>, Value<'a>>;
pub(crate) type IndexPageRef<'a> = SortedPageRef<'a, &'a [u8], Index>;
