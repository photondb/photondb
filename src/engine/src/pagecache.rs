use std::sync::Arc;

use crossbeam_epoch::Guard;

use crate::{OwnedPage, PageTable, SharedPage};

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
    table: PageTable,
}

impl PageCache {
    pub fn new() -> Self {
        Self {
            table: PageTable::new(),
        }
    }

    pub fn get<'a>(&self, id: PageId, _: &'a Guard) -> SharedPage<'a> {
        let id = id.into_usize();
        let ptr = self.table.get(id);
        SharedPage::from_usize(ptr).unwrap()
    }

    pub fn update<'a>(
        &self,
        id: PageId,
        old: SharedPage<'a>,
        new: OwnedPage,
        _: &'a Guard,
    ) -> Result<SharedPage<'a>, (SharedPage<'a>, OwnedPage)> {
        let id = id.into_usize();
        let old = old.into_usize();
        let new = new.into_usize();
        match self.table.cas(id, old, new) {
            Ok(_) => Ok(SharedPage::from_usize(new).unwrap()),
            Err(ptr) => unsafe {
                let old = SharedPage::from_usize(old).unwrap();
                let new = OwnedPage::from_usize(new).unwrap();
                Err((old, new))
            },
        }
    }

    pub fn replace<'a>(
        &self,
        id: PageId,
        old: SharedPage<'a>,
        new: OwnedPage,
        guard: &'a Guard,
    ) -> Result<SharedPage<'a>, (SharedPage<'a>, OwnedPage)> {
        let new = self.update(id, old, new, guard)?;
        let old = old.into_usize();
        guard.defer(move || {
            OwnedPage::from_usize(old);
        });
        Ok(new)
    }

    pub fn install<'a>(
        &self,
        page: OwnedPage,
        guard: &'a Guard,
    ) -> Result<(PageId, SharedPage<'a>), OwnedPage> {
        if let Some(id) = self.table.alloc(guard) {
            let page = page.into_shared();
            let ptr = page.into_usize();
            self.table.set(id, ptr);
            Ok((PageId::from_usize(id), page))
        } else {
            Err(page)
        }
    }

    pub fn uninstall<'a>(&self, id: PageId, guard: &'a Guard) {
        let id = id.into_usize();
        let ptr = self.table.get(id);
        self.table.dealloc(id, guard);
        guard.defer(move || {
            OwnedPage::from_usize(ptr);
        });
    }
}
