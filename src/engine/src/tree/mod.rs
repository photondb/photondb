mod error;
use error::{Error, Result};

mod btree;

mod pagetable;
use pagetable::PageTable;

mod pagestore;
use pagestore::{DiskAddr, DiskPtr, PageInfo, PageKind, PageStore};

#[derive(Copy, Clone, Debug)]
pub struct PageRef<'a>(&'a [u8]);

impl<'a> PageRef<'a> {
    pub fn ver(self) -> u64 {
        todo!()
    }

    pub fn len(self) -> u8 {
        todo!()
    }

    pub fn kind(self) -> PageKind {
        todo!()
    }

    pub fn next(self) -> PageAddr {
        todo!()
    }
}

impl From<u64> for PageRef<'_> {
    fn from(ptr: u64) -> Self {
        todo!()
    }
}

impl Into<u64> for PageRef<'_> {
    fn into(self) -> u64 {
        self.0.as_ptr() as u64
    }
}
pub enum PageView<'a> {
    Mem(PageRef<'a>),
    Disk(DiskAddr, PageInfo),
}

impl<'a> PageView<'a> {
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

    pub fn as_addr(&self) -> PageAddr {
        match *self {
            Self::Mem(page) => PageAddr::Mem(page.into()),
            Self::Disk(addr, _) => PageAddr::Disk(addr.into()),
        }
    }
}

pub enum PageAddr {
    Mem(u64),
    Disk(u64),
}

const MEM_DISK_MASK: u64 = 1 << 63;

impl From<u64> for PageAddr {
    fn from(addr: u64) -> Self {
        if addr & MEM_DISK_MASK == 0 {
            Self::Mem(addr)
        } else {
            Self::Disk(addr & !MEM_DISK_MASK)
        }
    }
}

impl<'a> Into<u64> for PageAddr {
    fn into(self) -> u64 {
        match self {
            Self::Mem(addr) => addr,
            Self::Disk(addr) => addr | MEM_DISK_MASK,
        }
    }
}
