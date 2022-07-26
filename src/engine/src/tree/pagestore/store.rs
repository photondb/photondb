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

    pub fn page_info(&self, offset: u64) -> Option<PageInfo> {
        todo!()
    }

    pub fn acquire_page(&self) -> u64 {
        todo!()
    }

    pub fn release_page(&self, offset: u64) {
        todo!()
    }

    pub async fn load_page(&self, offset: u64) -> Result<PagePtr> {
        todo!()
    }
}
