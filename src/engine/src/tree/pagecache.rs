use std::{
    alloc::{GlobalAlloc, Layout},
    sync::atomic::{AtomicUsize, Ordering},
};

use jemallocator::{usable_size, Jemalloc};

use super::{
    page::{PageAlloc, PagePtr, PageRef, PAGE_ALIGNMENT},
    pagestore::PageInfo,
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

    pub fn len(&self) -> u8 {
        match self {
            Self::Mem(page) => page.len(),
            Self::Disk(info, _) => info.len,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            Self::Mem(page) => page.is_leaf(),
            Self::Disk(info, _) => info.is_leaf,
        }
    }

    pub fn as_addr(&self) -> PageAddr {
        match self {
            Self::Mem(page) => PageAddr::Mem(page.as_ptr() as u64),
            Self::Disk(_, addr) => PageAddr::Disk(*addr),
        }
    }
}

pub struct PageCache {
    size: AtomicUsize,
    limit: usize,
}

impl Default for PageCache {
    fn default() -> Self {
        Self::with_limit(usize::MAX)
    }
}

impl PageCache {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            size: AtomicUsize::new(0),
            limit,
        }
    }
}

unsafe impl PageAlloc for PageCache {
    fn alloc(&self, size: usize) -> Option<PagePtr> {
        if self.size.load(Ordering::Relaxed) + size > self.limit {
            return None;
        }
        unsafe {
            let ptr = Jemalloc.alloc(alloc_layout(size));
            self.size.fetch_add(usable_size(ptr), Ordering::Relaxed);
            PagePtr::new(ptr)
        }
    }

    unsafe fn dealloc(&self, page: PagePtr) {
        let ptr = page.as_ptr();
        let size = usable_size(ptr);
        self.size.fetch_sub(size, Ordering::Relaxed);
        Jemalloc.dealloc(ptr, alloc_layout(size));
    }
}

unsafe fn alloc_layout(size: usize) -> Layout {
    Layout::from_size_align_unchecked(size, PAGE_ALIGNMENT)
}
