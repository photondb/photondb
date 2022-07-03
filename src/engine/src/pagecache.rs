use std::sync::Arc;

use crossbeam_epoch::Guard;

use crate::{Page, PageTable};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PageId(usize);

impl PageId {
    pub const fn zero() -> Self {
        Self(0)
    }

    fn from_usize(id: usize) -> Self {
        Self(id)
    }

    fn into_usize(self) -> usize {
        self.0
    }
}

pub struct PageCache {
    table: Arc<PageTable>,
}

impl PageCache {
    pub fn new() -> Self {
        Self {
            table: Arc::new(PageTable::new()),
        }
    }

    pub fn get<'a>(&self, id: PageId, _: &'a Guard) -> &'a Page {
        let id = id.into_usize();
        let ptr = self.table.get(id);
        unsafe { &*(ptr as *const Page) }
    }

    pub fn update<'a>(
        &self,
        id: PageId,
        old: &'a Page,
        new: Box<Page>,
        _: &'a Guard,
    ) -> Result<(), (&'a Page, Box<Page>)> {
        let id = id.into_usize();
        let old = old as *const Page;
        let new = Box::into_raw(new);
        if let Err(ptr) = self.table.cas(id, old as usize, new as usize) {
            unsafe {
                let old = &*(ptr as *const Page);
                let new = Box::from_raw(new);
                Err((old, new))
            }
        } else {
            Ok(())
        }
    }

    pub fn replace<'a>(
        &self,
        id: PageId,
        old: &'a Page,
        new: Box<Page>,
        guard: &'a Guard,
    ) -> Result<(), (&'a Page, Box<Page>)> {
        self.update(id, old, new, guard)?;
        let old = old as *const Page as usize;
        guard.defer(move || unsafe {
            Box::from_raw(old as *const Page as *mut Page);
        });
        Ok(())
    }

    pub fn install<'a>(&self, page: Box<Page>, _: &'a Guard) -> Option<PageId> {
        if let Some(id) = self.table.alloc() {
            let ptr = Box::into_raw(page) as usize;
            self.table.set(id, ptr);
            Some(PageId::from_usize(id))
        } else {
            None
        }
    }

    pub fn uninstall<'a>(&self, id: PageId, guard: &'a Guard) {
        let id = id.into_usize();
        let ptr = self.table.get(id);
        let table = self.table.clone();
        guard.defer(move || {
            table.dealloc(id);
            unsafe {
                Box::from_raw(ptr as *const Page as *mut Page);
            }
        });
    }
}
