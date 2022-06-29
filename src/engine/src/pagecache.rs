use crossbeam_epoch::Guard;

use crate::{PageId, PageTable};

pub struct Page {}

#[derive(Copy, Clone)]
pub struct PageRef<'a>(&'a Page);

impl PageRef<'_> {
    fn from_usize(ptr: usize) -> Self {
        Self(unsafe { &*(ptr as *const Page) })
    }

    fn into_usize(self) -> usize {
        self.0 as *const Page as _
    }
}

pub struct PageCache {
    table: PageTable,
}

impl PageCache {
    pub fn new() -> Self {
        Self {
            table: PageTable::new(),
        }
    }

    pub fn get<'a>(&self, id: PageId, _: &'a Guard) -> PageRef<'a> {
        let ptr = self.table.get(id);
        PageRef::from_usize(ptr)
    }

    pub fn cas<'a>(&self, id: PageId, old: PageRef<'a>, new: PageRef<'a>) -> Option<PageRef<'a>> {
        self.table
            .cas(id, old.into_usize(), new.into_usize())
            .map(|ptr| PageRef::from_usize(ptr))
    }

    pub fn alloc(&self, page: Page) -> PageRef<'_> {
        PageRef(Box::leak(Box::new(page)))
    }

    pub fn dealloc<'a>(&self, page: PageRef<'a>, guard: &'a Guard) {
        let ptr = page.into_usize();
        guard.defer(move || unsafe {
            Box::from_raw(ptr as *mut Page);
        })
    }

    pub fn install<'a>(&self, page: PageRef<'a>, guard: &'a Guard) -> Option<PageId> {
        self.table.install(page.into_usize(), guard)
    }
}
