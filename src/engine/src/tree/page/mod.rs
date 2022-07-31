mod base;
pub use base::{PageAlloc, PageBuilder, PageKind, PagePtr, PageVer};

mod iter;
pub use iter::{
    Comparable, ForwardIter, MergingIter, MergingIterBuilder, OptionIter, PrintableIter,
    RewindableIter, SeekableIter, SliceIter,
};

mod data;
pub use data::{Decodable, Encodable, Index, Key, Value};

mod data_page;
pub use data_page::{DataPageBuf, DataPageBuilder, DataPageIter, DataPageRef};

mod index_page;
pub use index_page::{IndexPageBuf, IndexPageBuilder, IndexPageIter, IndexPageRef};

mod delta_page;
pub use delta_page::{DeltaPageIter, DeltaPageRef};

mod sorted_page;
pub use sorted_page::{SortedPageBuf, SortedPageBuilder, SortedPageIter, SortedPageRef};

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
            PageKind::Delta => {
                if ptr.is_data() {
                    Self::Data(DataPageRef::new(ptr))
                } else {
                    Self::Index(IndexPageRef::new(ptr))
                }
            }
            PageKind::Split => Self::Split(SplitPageRef::new(ptr)),
        }
    }
}
