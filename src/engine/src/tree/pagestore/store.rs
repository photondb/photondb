use crate::tree::{
    page::{PageBuf, PageTag},
    Result,
};

pub struct PageInfo {
    pub ver: u64,
    pub len: u8,
    pub tag: PageTag,
    pub handle: PageHandle,
}

pub struct PageDesc {
    pub buf: PageBuf,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PageAddr(u64);

impl From<u64> for PageAddr {
    fn from(addr: u64) -> Self {
        Self(addr)
    }
}

impl From<PageAddr> for u64 {
    fn from(addr: PageAddr) -> u64 {
        addr.0
    }
}

pub struct PageHandle {
    fileno: u64,
    offset: u64,
    size: u64,
}

pub struct PageStore {}

impl PageStore {
    pub async fn open() -> Result<Self> {
        Ok(Self {})
    }

    pub fn page_info(&self, addr: PageAddr) -> Option<PageInfo> {
        todo!()
    }

    pub fn acquire_page(&self) -> PageAddr {
        todo!()
    }

    pub fn release_page(&self, addr: PageAddr, desc: PageDesc) {
        todo!()
    }

    pub fn highest_stable_addr(&self) -> PageAddr {
        todo!()
    }

    pub async fn load_page_with_addr(&self, addr: PageAddr) -> Result<PageBuf> {
        todo!()
    }

    pub async fn load_page_with_handle(&self, handle: &PageHandle) -> Result<PageBuf> {
        todo!()
    }
}
