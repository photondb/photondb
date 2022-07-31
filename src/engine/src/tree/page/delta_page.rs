use super::*;

pub trait DeltaPageRef: From<PagePtr> {
    type Key: Encodable + Decodable + Ord;
    type Value: Encodable + Decodable;
    type PageIter: DeltaPageIter<Key = Self::Key, Value = Self::Value>;

    fn iter(&self) -> Self::PageIter;
}

pub trait DeltaPageIter: From<PagePtr> + SeekableIter + RewindableIter
where
    Self::Key: Encodable + Decodable + Ord,
    Self::Value: Encodable + Decodable,
{
}

impl<T> DeltaPageIter for T
where
    T: From<PagePtr> + SeekableIter + RewindableIter,
    T::Key: Encodable + Decodable + Ord,
    T::Value: Encodable + Decodable,
{
}
