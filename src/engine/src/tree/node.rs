use super::{
    page::{DataPageIter, IndexPageIter, MergingIter, PagePtr, PageRef, PageTag},
    pagestore::{PageAddr, PageInfo},
};

pub type NodeId = u64;

pub enum PageView<'a> {
    Mem(PageRef<'a>),
    Disk(PageAddr, PageInfo),
}

impl<'a> PageView<'a> {
    pub fn ver(&self) -> u64 {
        match self {
            Self::Mem(page) => page.ver(),
            Self::Disk(_, page) => page.ver,
        }
    }

    pub fn len(&self) -> u8 {
        match self {
            Self::Mem(page) => page.len(),
            Self::Disk(_, page) => page.len,
        }
    }

    pub fn tag(&self) -> PageTag {
        match self {
            Self::Mem(page) => page.tag(),
            Self::Disk(_, page) => page.tag,
        }
    }

    pub fn as_ptr(&self) -> PagePtr {
        match *self {
            Self::Mem(page) => PagePtr::Mem(page.into()),
            Self::Disk(addr, _) => PagePtr::Disk(addr.into()),
        }
    }
}

pub struct NodePair<'a> {
    pub id: NodeId,
    pub view: PageView<'a>,
}

pub type DataNodeIter<'a> = MergingIter<DataPageIter<'a>>;
pub type IndexNodeIter<'a> = MergingIter<IndexPageIter<'a>>;
