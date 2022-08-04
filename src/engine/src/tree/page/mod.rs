mod base;
pub use base::{PageAlloc, PageBuilder, PageKind, PagePtr, PageRef};

mod data;
pub use data::{Comparable, Decodable, Encodable, Index, Key, Value};

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

mod util;
use util::{BufReader, BufWriter};

pub enum TypedPageRef<'a> {
    Data(DataPageRef<'a>),
    Index(IndexPageRef<'a>),
    Split(SplitPageRef<'a>),
}

impl<'a> From<PageRef<'a>> for TypedPageRef<'a> {
    fn from(page: PageRef<'a>) -> Self {
        match page.kind() {
            PageKind::Data => TypedPageRef::Data(page.into()),
            PageKind::Index => TypedPageRef::Index(page.into()),
            PageKind::Split => TypedPageRef::Split(page.into()),
        }
    }
}
