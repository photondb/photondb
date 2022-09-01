mod data;
pub use data::{Compare, DataItem, DecodeFrom, EncodeTo, Index, IndexItem, Key, Value};

mod iter;
pub use iter::{
    BoundedIter, ForwardIter, MergingIter, MergingIterBuilder, OptionIter, OrderedIter,
    SeekableIter, SliceIter,
};

mod base_page;
pub use base_page::{PageAlloc, PageBuilder, PageKind, PagePtr, PageRef};

mod data_page;
pub use data_page::{DataPageBuilder, DataPageIter, DataPageRef};

mod index_page;
pub use index_page::{IndexPageBuilder, IndexPageIter, IndexPageRef};

mod split_page;
pub use split_page::{SplitPageBuilder, SplitPageRef};

mod sorted_page;
pub use sorted_page::{SortedPageBuilder, SortedPageIter, SortedPageRef};

pub enum TypedPageRef<'a> {
    Data(DataPageRef<'a>),
    Index(IndexPageRef<'a>),
    Split(SplitPageRef<'a>),
}

impl<'a, T> From<T> for TypedPageRef<'a>
where
    T: Into<PageRef<'a>>,
{
    fn from(page: T) -> Self {
        let page = page.into();
        match page.kind() {
            PageKind::Base => {
                if page.is_leaf() {
                    Self::Data(page.into())
                } else {
                    Self::Index(page.into())
                }
            }
            PageKind::Split => Self::Split(page.into()),
        }
    }
}
