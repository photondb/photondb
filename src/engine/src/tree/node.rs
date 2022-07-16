use super::{
    page::{DataPageIter, IndexPageIter, MergeIter, PageKind, PagePtr, PageRef},
    pagestore::{PageAddr, PageInfo},
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NodeId(u64);

impl NodeId {
    pub const fn root() -> Self {
        Self(0)
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl Into<u64> for NodeId {
    fn into(self) -> u64 {
        self.0
    }
}

pub enum NodeView<'a> {
    Mem(PageRef<'a>),
    Disk(PageAddr, PageInfo),
}

impl<'a> NodeView<'a> {
    pub fn ver(&self) -> u64 {
        match self {
            Self::Mem(page) => page.ver(),
            Self::Disk(_, page) => page.ver,
        }
    }

    pub fn kind(&self) -> PageKind {
        match self {
            Self::Mem(page) => page.kind(),
            Self::Disk(_, page) => page.kind,
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
    pub view: NodeView<'a>,
}

impl<'a> NodePair<'a> {
    pub fn new(id: NodeId, view: NodeView<'a>) -> Self {
        Self { id, view }
    }

    pub fn ver(&self) -> u64 {
        self.view.ver()
    }

    pub fn is_data(&self) -> bool {
        self.view.kind().is_data()
    }
}

pub struct NodeIndex {
    pub id: NodeId,
    pub ver: u64,
}

impl NodeIndex {
    pub const fn root() -> Self {
        Self {
            id: NodeId::root(),
            ver: 0,
        }
    }
}

pub type DataNodeIter<'a> = MergeIter<DataPageIter<'a>>;
pub type IndexNodeIter<'a> = MergeIter<IndexPageIter<'a>>;
