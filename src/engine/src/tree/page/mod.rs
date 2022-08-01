mod base;
pub use base::{PageAlloc, PageBuilder, PageKind, PagePtr, PageRef};

mod iter;
pub use iter::{
    Comparable, DoubleEndedRewindableIter, DoubleEndedSeekableIter, ForwardIter, MergingIter,
    MergingIterBuilder, OptionIter, PrintableIter, RewindableIter, SeekableIter, SliceIter,
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
