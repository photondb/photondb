use crossbeam_epoch::Guard;

use crate::{PageBuf, PageRef, PageTable};

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

#[derive(Clone)]
pub struct PageCache {
    table: PageTable,
}

impl PageCache {
    pub fn new() -> Self {
        Self {
            table: PageTable::new(),
        }
    }

    pub fn get<'a>(&self, id: PageId, _: &'a Guard) -> Option<PageRef<'a>> {
        let id = id.into_usize();
        let ptr = self.table.get(id);
        PageRef::from_usize(ptr)
    }

    pub fn update<'a>(
        &self,
        id: PageId,
        old: PageRef<'a>,
        new: PageBuf,
        _: &'a Guard,
    ) -> Result<PageRef<'a>, (PageRef<'a>, PageBuf)> {
        let id = id.into_usize();
        let old = old.into_usize();
        let new = new.into_usize();
        match self.table.cas(id, old, new) {
            Ok(_) => Ok(unsafe { PageRef::from_usize_unchecked(new) }),
            Err(now) => unsafe {
                let now = PageRef::from_usize_unchecked(now);
                let new = PageBuf::from_usize_unchecked(new);
                Err((now, new))
            },
        }
    }

    pub fn replace<'a>(
        &self,
        id: PageId,
        old: PageRef<'a>,
        new: PageBuf,
        guard: &'a Guard,
    ) -> Result<PageRef<'a>, (PageRef<'a>, PageBuf)> {
        let new = self.update(id, old, new, guard)?;
        let old = old.into_usize();
        guard.defer(move || {
            PageBuf::from_usize(old);
        });
        Ok(new)
    }

    pub fn attach<'a>(&self, page: PageRef<'a>, guard: &'a Guard) -> Result<PageId, ()> {
        let id = self.table.alloc(guard).ok_or(())?;
        let ptr = page.into_usize();
        self.table.set(id, ptr);
        Ok(PageId::from_usize(id))
    }

    pub fn detach(&self, id: PageId, guard: &Guard) {
        let id = id.into_usize();
        self.table.dealloc(id, guard);
    }

    pub fn install<'a>(
        &self,
        page: PageBuf,
        guard: &'a Guard,
    ) -> Result<(PageId, PageRef<'a>), PageBuf> {
        if let Some(id) = self.table.alloc(guard) {
            let ptr = page.into_usize();
            self.table.set(id, ptr);
            let id = PageId::from_usize(id);
            let page = unsafe { PageRef::from_usize_unchecked(ptr) };
            Ok((id, page))
        } else {
            Err(page)
        }
    }

    pub fn uninstall<'a>(&self, id: PageId, guard: &'a Guard) {
        let id = id.into_usize();
        let ptr = self.table.get(id);
        self.table.dealloc(id, guard);
        guard.defer(move || {
            PageBuf::from_usize(ptr);
        });
    }
}
