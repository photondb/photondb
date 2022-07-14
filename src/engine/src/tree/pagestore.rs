use super::{Options, PageKind, Result};

pub struct PageInfo {
    pub ver: u64,
    pub kind: PageKind,
    pub handle: DiskHandle,
}

pub struct PageDesc {
    pub ver: u64,
    pub kind: PageKind,
    pub data: Vec<u8>,
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

pub struct DiskHandle {
    fileno: u64,
    offset: u64,
    size: u64,
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

    pub fn acquire_page(&self) -> DiskAddr {
        todo!()
    }

    pub fn release_page(&self, addr: DiskAddr, desc: PageDesc) {
        todo!()
    }

    pub fn highest_stable_addr(&self) -> DiskAddr {
        todo!()
    }

    pub async fn load_page_with_addr(&self, addr: DiskAddr) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub async fn load_page_with_handle(&self, handle: DiskHandle) -> Result<Option<Vec<u8>>> {
        todo!()
    }
}
