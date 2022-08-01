mod base;
pub use base::{PageAlloc, PageBuilder, PageKind, PagePtr};

mod iter;
pub use iter::{
    Comparable, ForwardIter, MergingIter, MergingIterBuilder, OptionIter, PrintableIter,
    RewindableIter, SeekableIter, SliceIter,
};

mod data;
pub use data::{Decodable, Encodable, Index, Key, Value};

mod data_page;
pub use data_page::{DataPageBuilder, DataPageIter, DataPageRef};

mod index_page;
pub use index_page::{IndexPageBuilder, IndexPageIter, IndexPageRef};

mod sorted_page;
pub use sorted_page::{SortedPageBuilder, SortedPageIter, SortedPageRef};

mod split_page;
pub use split_page::{SplitPageBuilder, SplitPageRef};

mod util;
use util::{BufReader, BufWriter};

/// An aggregated type with all kinds of page references.
pub enum PageRef<'a> {
    Data(DataPageRef<'a>),
    Index(IndexPageRef<'a>),
    Split(SplitPageRef<'a>),
}

impl<'a> PageRef<'a> {
    /// Creates a `PageRef` from a `PagePtr`.
    pub fn cast(ptr: PagePtr) -> Self {
        match ptr.kind() {
            PageKind::Data => Self::Data(DataPageRef::new(ptr)),
            PageKind::Index => Self::Index(IndexPageRef::new(ptr)),
            PageKind::Split => Self::Split(SplitPageRef::new(ptr)),
        }
    }
}

pub trait PageIter: SeekableIter + RewindableIter
where
    Self::Key: Encodable + Decodable + Ord,
    Self::Value: Encodable + Decodable,
{
}

impl<T> PageIter for T
where
    T: SeekableIter + RewindableIter,
    T::Key: Encodable + Decodable + Ord,
    T::Value: Encodable + Decodable,
{
}
