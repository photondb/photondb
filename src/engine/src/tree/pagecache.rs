use std::{
    alloc::GlobalAlloc,
    sync::atomic::{AtomicUsize, Ordering},
};

use jemallocator::{usable_size, Jemalloc};

use super::{
    page::{PageAlloc, PagePtr, PageRef, PageTags},
    pagestore::{PageInfo, PageStore},
    pagetable::PageTable,
    Ghost, Options, Result,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

impl From<PageAddr> for u64 {
    fn from(addr: PageAddr) -> u64 {
        match addr {
            PageAddr::Mem(addr) => addr,
            PageAddr::Disk(addr) => addr | MEM_DISK_MASK,
        }
    }
}

pub enum PageView<'a> {
    Mem(PageRef<'a>),
    Disk(PageInfo, u64),
}

impl<'a> PageView<'a> {
    pub fn ver(&self) -> u64 {
        match self {
            Self::Mem(page) => page.ver(),
            Self::Disk(info, _) => info.ver,
        }
    }

    pub fn tags(&self) -> PageTags {
        match self {
            Self::Mem(page) => page.tags(),
            Self::Disk(info, _) => info.tags,
        }
    }

    pub fn chain_len(&self) -> u8 {
        match self {
            Self::Mem(page) => page.chain_len(),
            Self::Disk(info, _) => info.chain_len,
        }
    }
}

pub struct PageCache {
    alloc: LimitedAlloc,
    table: PageTable,
    store: PageStore,
}

impl PageCache {
    pub async fn open(opts: Options) -> Result<Self> {
        let alloc = LimitedAlloc::with_limit(opts.cache_size);
        let table = PageTable::default();
        let store = PageStore::open(opts).await?;
        Ok(Self {
            alloc,
            table,
            store,
        })
    }

    pub fn page_view(&self, addr: PageAddr) -> Option<PageView> {
        match addr {
            PageAddr::Mem(addr) => unsafe {
                PageRef::from_raw(addr as *const u8).map(PageView::Mem)
            },
            PageAddr::Disk(addr) => self
                .store
                .page_info(addr)
                .map(|info| PageView::Disk(info, addr)),
        }
    }

    pub async fn swapin_page<'a>(&self, addr: u64, ghost: &'a Ghost) -> Result<PageRef<'a>> {
        let page = self.store.load_page(addr).await?;
        Ok(page.into())
    }

    pub async fn load_page_with_addr<'a>(
        &self,
        addr: PageAddr,
        ghost: &'a Ghost,
    ) -> Result<Option<PageRef<'a>>> {
        match addr {
            PageAddr::Mem(addr) => unsafe { Ok(PageRef::from_raw(addr as *const u8)) },
            PageAddr::Disk(addr) => self.swapin_page(addr, ghost).await.map(Some),
        }
    }

    pub async fn load_page_with_view<'a>(
        &self,
        view: PageView<'a>,
        ghost: &'a Ghost,
    ) -> Result<PageRef<'a>> {
        match view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(_, addr) => self.swapin_page(addr, ghost).await,
        }
    }
}

struct LimitedAlloc {
    size: AtomicUsize,
    limit: usize,
}

impl Default for LimitedAlloc {
    fn default() -> Self {
        Self::with_limit(usize::MAX)
    }
}

impl LimitedAlloc {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            size: AtomicUsize::new(0),
            limit,
        }
    }
}

unsafe impl PageAlloc for LimitedAlloc {
    unsafe fn alloc(&self, size: usize) -> Option<PagePtr> {
        if self.size.load(Ordering::Relaxed) + size > self.limit {
            None
        } else {
            let ptr = Jemalloc.alloc(Self::alloc_layout(size));
            self.size.fetch_add(usable_size(ptr), Ordering::Relaxed);
            PagePtr::from_raw(ptr)
        }
    }

    unsafe fn dealloc(&self, page: PagePtr) {
        let ptr = page.into_raw();
        let size = usable_size(ptr);
        self.size.fetch_sub(size, Ordering::Relaxed);
        Jemalloc.dealloc(ptr, Self::alloc_layout(size));
    }
}
