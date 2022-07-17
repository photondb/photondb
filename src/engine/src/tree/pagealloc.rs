use std::{
    alloc::GlobalAlloc,
    sync::atomic::{AtomicUsize, Ordering},
};

use jemallocator::{usable_size, Jemalloc};

use super::page::{PageBuf, PageLayout};

pub struct PageAlloc {
    size: AtomicUsize,
}

impl Default for PageAlloc {
    fn default() -> Self {
        Self {
            size: AtomicUsize::new(0),
        }
    }
}

impl PageAlloc {
    pub fn alloc<L: PageLayout, B: From<PageBuf>>(&self, layout: &L) -> B {
        unsafe {
            let buf = Jemalloc.alloc(layout.layout());
            self.size.fetch_add(usable_size(buf), Ordering::Relaxed);
        }
        todo!()
    }

    pub fn dealloc(&self, page: PageBuf) {
        todo!()
    }
}
