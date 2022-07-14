use super::{DiskAddr, PageInfo};

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

    pub fn from_addr(addr: u64) -> Option<PageRef<'a>> {
        todo!()
    }
}

impl<'a> From<u64> for PageRef<'a> {
    fn from(addr: u64) -> Self {
        todo!()
    }
}

impl<'a> Into<u64> for PageRef<'a> {
    fn into(self) -> u64 {
        todo!()
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

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum PageKind {
    Base = 0,
}

impl PageKind {
    pub fn is_data(&self) -> bool {
        todo!()
    }
}
