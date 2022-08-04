use std::{
    alloc::GlobalAlloc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use jemallocator::{usable_size, Jemalloc};

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
unsafe impl PageAlloc for PageCache {
    type Error = Error;

    fn alloc(&self, size: usize) -> Result<PagePtr> {
        self.size.fetch_add(size, Ordering::Relaxed);
        BuiltinAlloc.alloc(size).map_err(|_| Error::Alloc)
    }

    unsafe fn dealloc(&self, page: PagePtr) {
        let size = BuiltinAlloc::usable_size(page.as_raw());
        self.size.fetch_sub(size, Ordering::Relaxed);
        BuiltinAlloc.dealloc(page);
    }
}

#[cfg(not(miri))]
unsafe impl PageAlloc for PageCache {
    type Error = Error;

    fn alloc(&self, size: usize) -> Result<PagePtr> {
        unsafe {
            let ptr = Jemalloc.alloc(Self::alloc_layout(size));
            let size = usable_size(ptr);
            self.size.fetch_add(size, Ordering::Relaxed);
            PagePtr::new(ptr).ok_or(Error::Alloc)
        }
    }

    unsafe fn dealloc(&self, page: PagePtr) {
        let ptr = page.as_raw();
        let size = usable_size(ptr);
        self.size.fetch_sub(size, Ordering::Relaxed);
        Jemalloc.dealloc(ptr, Self::alloc_layout(size));
    }
}
