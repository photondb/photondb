use std::{
    alloc::GlobalAlloc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use super::{page::*, Error, Result};
use crate::util::SizedAlloc;

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
use crate::util::Jemalloc as Alloc;
#[cfg(miri)]
use crate::util::Sysalloc as Alloc;

const ALLOC: Alloc = Alloc;

unsafe impl PageAlloc for PageCache {
    type Error = Error;

    fn alloc_page(&self, size: usize) -> Result<PagePtr> {
        unsafe {
            let ptr = ALLOC.alloc(PagePtr::layout(size));
            let size = ALLOC.alloc_size(ptr);
            self.size.fetch_add(size, Ordering::Relaxed);
            PagePtr::new(ptr).ok_or(Error::Alloc)
        }
    }

    unsafe fn dealloc_page(&self, page: PagePtr) {
        let ptr = page.as_raw();
        let size = ALLOC.alloc_size(ptr);
        self.size.fetch_sub(size, Ordering::Relaxed);
        ALLOC.dealloc(ptr, PagePtr::layout(size));
    }
}
