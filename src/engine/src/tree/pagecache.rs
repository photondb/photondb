use std::{
    alloc::GlobalAlloc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use super::{page::*, Error, Result};

#[derive(Clone)]
pub struct PageCache {
    size: Arc<AtomicUsize>,
}

impl Default for PageCache {
    fn default() -> Self {
        Self {
            size: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl PageCache {
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
}

#[cfg(miri)]
use crate::util::{SizedAlloc, Sysalloc};

#[cfg(miri)]
unsafe impl PageAlloc for PageCache {
    type Error = Error;

    fn alloc_page(&self, size: usize) -> Result<PagePtr> {
        unsafe {
            let ptr = Sysaloc.alloc(Self::page_layout(size));
            let size = Sysaloc.alloc_size(ptr);
            self.size.fetch_add(size, Ordering::Relaxed);
            PagePtr::new(ptr).ok_or(Error::Alloc)
        }
    }

    unsafe fn dealloc_page(&self, page: PagePtr) {
        let ptr = page.as_raw();
        let size = Sysaloc.alloc_size(ptr);
        self.size.fetch_sub(size, Ordering::Relaxed);
        Sysaloc.dealloc(ptr, Self::page_layout(size));
    }
}

#[cfg(not(miri))]
use crate::util::{Jemalloc, SizedAlloc};

#[cfg(not(miri))]
unsafe impl PageAlloc for PageCache {
    type Error = Error;

    fn alloc_page(&self, size: usize) -> Result<PagePtr> {
        unsafe {
            let ptr = Jemalloc.alloc(Self::page_layout(size));
            let size = Jemalloc.alloc_size(ptr);
            self.size.fetch_add(size, Ordering::Relaxed);
            PagePtr::new(ptr).ok_or(Error::Alloc)
        }
    }

    unsafe fn dealloc_page(&self, page: PagePtr) {
        let ptr = page.as_raw();
        let size = Jemalloc.alloc_size(ptr);
        self.size.fetch_sub(size, Ordering::Relaxed);
        Jemalloc.dealloc(ptr, Self::page_layout(size));
    }
}
