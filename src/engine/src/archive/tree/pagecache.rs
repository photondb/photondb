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

#[cfg(not(miri))]
mod alloc {
    use jemallocator::{usable_size, Jemalloc};

    use super::*;

    unsafe impl PageAlloc for PageCache {
        type Error = Error;

        fn alloc_page(&self, size: usize) -> Result<PagePtr> {
            unsafe {
                let ptr = Jemalloc.alloc(PagePtr::layout(size));
                let size = usable_size(ptr);
                self.size.fetch_add(size, Ordering::Relaxed);
                PagePtr::new(ptr).ok_or(Error::Alloc)
            }
        }

        unsafe fn dealloc_page(&self, page: PagePtr) {
            let ptr = page.as_raw();
            let size = usable_size(ptr);
            self.size.fetch_sub(size, Ordering::Relaxed);
            Jemalloc.dealloc(ptr, PagePtr::layout(size));
        }
    }
}

#[cfg(miri)]
mod alloc {
    use std::alloc::System;

    use super::*;

    unsafe impl PageAlloc for PageCache {
        type Error = Error;

        fn alloc_page(&self, size: usize) -> Result<PagePtr> {
            unsafe {
                let ptr = System.alloc(PagePtr::layout(size));
                self.size.fetch_add(size, Ordering::Relaxed);
                PagePtr::new(ptr).ok_or(Error::Alloc)
            }
        }

        unsafe fn dealloc_page(&self, page: PagePtr) {
            let size = page.size();
            self.size.fetch_sub(size, Ordering::Relaxed);
            System.dealloc(page.as_raw(), PagePtr::layout(size));
        }
    }
}
