use crate::tree::{
    page::{PagePtr, PageTags},
    Options, Result,
};

pub struct PageInfo {
    pub ver: u64,
    pub tags: PageTags,
    pub chain_len: u8,
}

pub struct PageStore {
    opts: Options,
}

impl PageStore {
    pub async fn open(opts: Options) -> Result<Self> {
        Ok(Self { opts })
    }

    pub fn page_info(&self, addr: u64) -> Option<PageInfo> {
        todo!()
    }

    pub async fn load_page(&self, addr: u64) -> Result<PagePtr> {
        todo!()
    }

    pub fn acquire_page(&self) -> u64 {
        todo!()
    }

    pub fn release_page(&self, addr: u64) {
        todo!()
    }
}
