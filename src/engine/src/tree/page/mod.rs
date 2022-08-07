mod base;
pub use base::{PageAlloc, PageBuilder, PageKind, PagePtr, PageRef};

mod data;
pub use data::{Compare, DecodeFrom, EncodeTo, Index, Key, Value};

mod iter;
pub use iter::{
    BoundedIter, ForwardIter, MergingIter, MergingIterBuilder, OptionIter, RewindableIter,
    SeekableIter, SliceIter,
};

mod data_page;
pub use data_page::{DataItem, DataPageBuilder, DataPageIter, DataPageRef};

mod index_page;
pub use index_page::{IndexItem, IndexPageBuilder, IndexPageIter, IndexPageRef};

mod split_page;
pub use split_page::{SplitPageBuilder, SplitPageRef};

mod sorted_page;
pub use sorted_page::{SortedPageBuilder, SortedPageIter, SortedPageRef};

pub enum TypedPage<'a> {
    Data(DataPageRef<'a>),
    Index(IndexPageRef<'a>),
    Split(SplitPageRef<'a>),
}

impl<'a> From<PageRef<'a>> for TypedPage<'a> {
    fn from(page: PageRef<'a>) -> Self {
        match page.kind() {
            PageKind::Data => Self::Data(page.into()),
            PageKind::Index => Self::Index(page.into()),
            PageKind::Split => Self::Split(page.into()),
        }
    }
}
