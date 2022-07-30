use std::marker::PhantomData;

use super::{page::*, pagecache::PageView};

pub struct Node<'a> {
    pub id: u64,
    pub view: PageView,
    _mark: PhantomData<&'a ()>,
}

impl Node<'_> {
    pub fn new(id: u64, view: PageView) -> Self {
        Self {
            id,
            view,
            _mark: PhantomData,
        }
    }
}

pub trait NodeKind {
    type Key: Encodable + Decodable + Ord;
    type Value: Encodable + Decodable;
    type PageRef: From<PagePtr>;
    type PageIter: From<PagePtr> + PageIter<Key = Self::Key, Value = Self::Value>;
    type NodeIter: From<MergingIter<Self::PageIter>>
        + PageIter<Key = Self::Key, Value = Self::Value>;
}

pub struct LeafNode<'a>(PhantomData<&'a ()>);

impl<'a> NodeKind for LeafNode<'a> {
    type Key = Key<'a>;
    type Value = Value<'a>;
    type PageRef = DataPageRef<'a, Self::Key, Self::Value>;
    type PageIter = DataPageIter<'a, Self::Key, Self::Value>;
    type NodeIter = MergingIter<Self::PageIter>;
}

pub struct IndexNode<'a>(PhantomData<&'a ()>);

impl<'a> NodeKind for IndexNode<'a> {
    type Key = &'a [u8];
    type Value = Index;
    type PageRef = DataPageRef<'a, Self::Key, Self::Value>;
    type PageIter = DataPageIter<'a, Self::Key, Self::Value>;
    type NodeIter = MergingIter<Self::PageIter>;
}
