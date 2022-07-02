use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

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
    table: Arc<Table>,
}

impl PageCache {
    pub fn new() -> Self {
        Self {
            table: Arc::new(Table::new()),
        }
    }

    pub fn get<'a>(&self, id: PageId, _: &'a Guard) -> SharedPage<'a> {
        let id = id.into_usize();
        let ptr = self.table.get(id);
        SharedPage::from_usize(ptr)
    }

    pub fn update<'a>(
        &self,
        id: PageId,
        old: SharedPage<'a>,
        new: OwnedPage,
        _: &'a Guard,
    ) -> Result<(), (SharedPage<'a>, OwnedPage)> {
        let id = id.into_usize();
        let old = old.into_usize();
        let new = new.into_usize();
        self.table
            .cas(id, old, new)
            .map(|_| ())
            .map_err(|ptr| (SharedPage::from_usize(ptr), OwnedPage::from_usize(new)))
    }

    pub fn replace<'a>(
        &self,
        id: PageId,
        old: SharedPage<'a>,
        new: OwnedPage,
        guard: &'a Guard,
    ) -> Result<(), (SharedPage<'a>, OwnedPage)> {
        self.update(id, old, new, guard)?;
        let mut cursor = Some(old);
        while let Some(page) = cursor {
            cursor = page.next();
            page.into_owned();
        }
        Ok(())
    }

    pub fn install<'a>(&self, page: OwnedPage, guard: &'a Guard) -> Option<PageId> {
        if let Some(id) = self.table.alloc() {
            self.table.set(id, page.into_usize());
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
            OwnedPage::from_usize(ptr);
        });
    }
}

struct Table {
    map: PageTable,
    free: AtomicUsize,
}

impl Table {
    fn new() -> Self {
        Self {
            map: PageTable::new(),
            free: AtomicUsize::new(0),
        }
    }

    fn get(&self, id: usize) -> usize {
        self.map[id].load(Ordering::Acquire)
    }

    fn set(&self, id: usize, ptr: usize) {
        self.map[id].store(ptr, Ordering::Release);
    }

    fn cas(&self, id: usize, old: usize, new: usize) -> Result<usize, usize> {
        self.map[id].compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
    }

    fn alloc(&self) -> Option<usize> {
        let mut id = self.free.load(Ordering::Acquire);
        while id != 0 {
            let next = self.map[id].load(Ordering::Acquire);
            match self
                .free
                .compare_exchange(id, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => id = actual,
            }
        }
        if id != 0 {
            Some(id)
        } else {
            self.map.alloc()
        }
    }

    fn dealloc(&self, id: usize) {
        let mut next = self.free.load(Ordering::Acquire);
        loop {
            self.map[id].store(next, Ordering::Release);
            match self
                .free
                .compare_exchange(next, id, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => next = actual,
            }
        }
    }
}
