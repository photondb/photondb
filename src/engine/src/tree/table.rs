use crossbeam_epoch::Guard;

use super::{cache::PageTable, store::PageStore};
use crate::Result;

pub struct Options {
    pub node_size: usize,
    pub delta_chain_length: usize,
}

pub struct Table {}

impl Table {
    pub fn new(opts: Options) -> Self {
        Self {}
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        todo!()
    }
}

struct Inner {
    table: PageTable,
    store: PageStore,
}

impl Inner {
    async fn get<'k, 'g>(&self, key: &'k [u8], guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        let page = self.locate_data_page(key, guard).await;
        self.lookup_data_in_page(key, page.ptr, guard).await
    }

    async fn put<'g>(&self, key: &[u8], value: &[u8], guard: &'g Guard) -> Result<()> {
        loop {
            let page = self.locate_data_page(key, guard).await;
            let delta = self.alloc_page(page.epoch(), PageLayout::Put(key, value));
            self.update(page, delta, guard).await?;
        }
    }

    async fn delete<'g>(&self, key: &[u8], guard: &'g Guard) -> Result<()> {
        loop {
            let page = self.locate_data_page(key, guard).await;
            let delta = self.alloc_page(page.epoch(), PageLayout::Delete(key));
            self.update(page, delta, guard).await?;
        }
    }

    fn update<'g>(&self, page: PageView<'g>, delta: PageBuf, guard: &'g Guard) -> Result<()> {
        let pid = page.id.into();
        let mut old_ptr = page.ptr.into();
        let new_ptr = delta.as_ptr();
        loop {
            match self.table.cas(pid, old_ptr, new_ptr, guard) {
                Ok(_) => return Ok(()),
                Err(now_ptr) => {
                    let ptr = PagePtr::from(now_ptr);
                    if let PagePtr::Mem(page) = ptr {
                        // TODO
                    }
                }
            }
        }
        todo!()
    }

    fn get_page<'g>(&self, pid: PageId, guard: &'g Guard) -> PagePtr<'g> {
        self.table.get(pid.into()).into()
    }

    async fn deref_page<'g>(&self, ptr: PagePtr<'g>, guard: &'g Guard) -> PageRef<'g> {
        match ptr {
            PagePtr::Mem(page) => page,
            PagePtr::Disk(addr) => todo!(),
        }
    }

    async fn locate_data_page<'g>(&self, key: &[u8], guard: &'g Guard) -> PageView<'g> {
        loop {
            if let Some(page) = self.try_locate_data_page(key, guard).await {
                return page;
            }
        }
    }

    async fn try_locate_data_page<'g>(&self, key: &[u8], guard: &'g Guard) -> Option<PageView<'g>> {
        let mut pid = PageId::root();
        let mut epoch = 0;
        let mut parent = None;
        loop {
            let ptr = self.get_page(pid, guard);
            ptr.epoch() != epoch {
                // self.handle_pending_smo(pid, ptr, guard);
            }
            if ptr.is_data() {
                return PageView::new(pid, ptr);
            }
            let page = self.deref_page(ptr, guard).await;
            let child = self.lookup_child_in_page(key, page, guard).await;
        }
        todo!()
    }

    async fn lookup_data_in_page<'g>(
        &self,
        key: &[u8],
        page: PageRef<'g>,
        guard: &'g Guard,
    ) -> Result<Option<&'g [u8]>> {
        let mut cursor = ptr;
        loop {
            match cursor.content() {
                _ => todo!(),
            }
            if let Some(next) = cursor.next() {
                cursor = next;
            } else {
                break;
            }
        }
    }

    async fn lookup_child_in_page<'g>(
        &self,
        key: &[u8],
        page: PageRef<'g>,
        guard: &'g Guard,
    ) -> Result<PageHandle> {
        todo!()
    }

    fn alloc_page(&self, layout: PageLayout, version: u64) -> PageBuf {
        todo!()
    }

    fn dealloc_page(&self, page: PageBuf) {
        todo!()
    }

    fn flush_page(&self) {
        todo!()
    }

    fn swapout_page(&self) {
        todo!()
    }

    fn swapout_pages(&self, size: u64) {
        todo!()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct PageId(u64);

impl PageId {
    const fn root() -> Self {
        Self(0)
    }
}

impl From<u64> for PageId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl Into<u64> for PageId {
    fn into(self) -> u64 {
        self.0
    }
}

const MEM_DISK_MASK: u64 = 1 << 63;

enum PagePtr<'a> {
    Mem(PageRef<'a>),
    Disk(u64),
}

impl<'a> From<u64> for PagePtr<'a> {
    fn from(ptr: u64) -> Self {
        if ptr & MEM_DISK_MASK == 0 {
            todo!()
        } else {
            todo!()
        }
    }
}

impl<'a> Into<u64> for PagePtr<'a> {
    fn into(self) -> u64 {
        todo!()
    }
}

struct PageRef<'a>(&'a [u8]);

impl<'a> PageRef<'a> {
    fn next(self) -> Option<PagePtr<'a>> {

    }

    fn kind(self) -> PageKind {
        todo!()
    }

    fn epoch(self) -> u64 {
        todo!()
    }

    fn chain_length(self) -> usize {
        todo!()
    }
}

struct PageBuf();

struct PageView<'g> {
    pid: PageId,
    ptr: PagePtr<'g>,
}

struct DiskPtr {
    addr: DiskAddr,
    kind: PageKind,
    ver: PageVersion,
}

struct PageHandle {
    pub pid: PageId,
    pub ver: PageVer,
}

type PageVer = u64;

#[repr(u8)]
enum PageKind {
    BaseData = 0,
    BaseIndex = 32,
}

impl PageKind {
    fn is_data(self) -> bool {
        todo!()
    }
}

enum PageLayout<'a> {
    Put(&'a [u8], &'a [u8]),
    Delete(&'a [u8]),
}