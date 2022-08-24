use std::path::PathBuf;

use super::file_system::FileSystem;
use crate::{
    env::Env,
    tree::{page::PagePtr, Options, Result},
};

#[derive(Copy, Clone, Debug)]
pub struct PageInfo {
    pub ver: u64,
    pub len: u8,
    pub is_leaf: bool,
}

pub struct PageStore<E: Env> {
    fs: FileSystem<E>,
    opts: Options,
}

#[allow(dead_code)]
impl<E: Env> PageStore<E> {
    pub async fn open(env: E, root: PathBuf, opts: Options) -> Result<Self> {
        let fs = FileSystem::open(env, root).await?;
        Ok(Self { fs, opts })
    }

    pub fn page_info(&self, _addr: u64) -> Option<PageInfo> {
        todo!()
    }

    pub fn load_page(&self, _addr: u64) -> Result<PagePtr> {
        todo!()
    }

    pub fn acquire_page(&self) -> u64 {
        todo!()
    }

    pub fn release_page(&self, _addr: u64) {
        todo!()
    }
}
