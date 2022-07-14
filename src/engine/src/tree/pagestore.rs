use super::{Options, Result};

pub struct DiskPtr {
    fileno: u64,
    offset: u64,
    size: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct DiskAddr(u64);

impl From<u64> for DiskAddr {
    fn from(addr: u64) -> Self {
        Self(addr)
    }
}

impl Into<u64> for DiskAddr {
    fn into(self) -> u64 {
        self.0
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

pub struct PageInfo {
    pub ptr: DiskPtr,
    pub ver: u64,
    pub kind: PageKind,
}

pub struct PageStore {
    opts: Options,
}

impl PageStore {
    pub async fn open(opts: Options) -> Result<Self> {
        Ok(Self { opts })
    }

    pub fn page_info(&self, addr: DiskAddr) -> Option<PageInfo> {
        todo!()
    }

    pub async fn load_page_with_ptr(&self, ptr: DiskPtr) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub async fn load_page_with_addr(&self, addr: DiskAddr) -> Result<Option<Vec<u8>>> {
        todo!()
    }
}
