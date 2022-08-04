mod base;
pub use base::{PageAlloc, PageBuilder, PageKind, PagePtr, PageRef};

mod iter;
pub use iter::{
    ForwardIter, MergingIter, MergingIterBuilder, OptionIter, RewindableIter, SeekableIter,
    SliceIter,
};

mod data;
pub use data::{Comparable, Decodable, Encodable, Index, Key, Value};

/*
mod data_page;
pub use data_page::{DataPageBuilder, DataPageIter, DataPageRef};

mod index_page;
pub use index_page::{IndexPageBuilder, IndexPageIter, IndexPageRef};

mod split_page;
pub use split_page::{SplitPageBuilder, SplitPageRef};
*/

mod sorted_page;
pub use sorted_page::{SortedPageBuilder, SortedPageIter, SortedPageRef};

mod util;
use util::{BufReader, BufWriter};

pub trait PageIter: SeekableIter + RewindableIter
where
    Self::Item: Encodable + Decodable + Ord,
{
}

impl<T> PageIter for T
where
    T: SeekableIter + RewindableIter,
    T::Item: Encodable + Decodable + Ord,
{
}
