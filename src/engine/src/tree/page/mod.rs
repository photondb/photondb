mod base;
pub use base::{PageAlloc, PageBuilder, PageKind, PagePtr, PageVer};

mod iter;
pub use iter::{
    ForwardIter, MergingIter, MergingIterBuilder, OptionIter, PrintableIter, RewindableIter,
    SeekableIter, SliceIter,
};

mod data;
pub use data::{Decodable, Encodable, Index, Key, Value};

mod delta_page;
pub use delta_page::{DeltaPageBuilder, DeltaPageIter, DeltaPageRef};

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
    pub fn cast(base: PagePtr) -> Self {
        match base.kind() {
            PageKind::Delta => {
                if base.is_data() {
                    Self::Data(DeltaPageRef::new(base))
                } else {
                    Self::Index(DeltaPageRef::new(base))
                }
            }
            PageKind::Split => Self::Split(SplitPageRef::new(base)),
        }
    }
}

pub type DataPageRef<'a> = DeltaPageRef<'a, Key<'a>, Value<'a>>;
pub type IndexPageRef<'a> = DeltaPageRef<'a, &'a [u8], Index>;

/// An aggregated trait that satisfies multiple iterator traits.
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
