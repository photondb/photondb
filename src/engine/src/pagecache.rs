use crossbeam_epoch::Guard;

use crate::{PageContent, PageId, PageTable};

pub struct Page {
    header: PageHeader,
    content: PageContent,
}

impl Page {
    pub fn new(header: PageHeader, content: PageContent) -> Self {
        Self { header, content }
    }

    pub fn with_next(next: PageRef<'_>, content: PageContent) -> Self {
        let mut header = next.0.header.clone();
        header.next = next.into_usize();
        Self { header, content }
    }
}

#[derive(Clone)]
pub struct PageHeader {
    next: usize,
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
}

impl PageHeader {
    pub fn new() -> Self {
        Self {
            next: 0,
            lower_bound: Vec::new(),
            upper_bound: Vec::new(),
        }
    }

    pub fn covers(&self, key: &[u8]) -> bool {
        key >= &self.lower_bound && (key < &self.upper_bound || self.upper_bound.is_empty())
    }
}

#[derive(Copy, Clone)]
pub struct PageRef<'a>(&'a Page);

impl<'a> PageRef<'a> {
    fn from_usize(ptr: usize) -> Self {
        Self(unsafe { &*(ptr as *const Page) })
    }

    fn into_usize(self) -> usize {
        self.0 as *const Page as _
    }

    fn header(self) -> &'a PageHeader {
        &self.0.header
    }

    fn content(self) -> &'a PageContent {
        &self.0.content
    }

    pub fn next(self) -> Option<PageRef<'a>> {
        let next = self.0.header.next;
        if next == 0 {
            None
        } else {
            Some(PageRef::from_usize(next))
        }
    }

    pub fn covers(self, key: &[u8]) -> bool {
        self.0.header.covers(key)
    }

    pub fn is_data(self) -> bool {
        self.0.content.is_data()
    }

    pub fn lookup_data(self, key: &[u8]) -> Option<&'a [u8]> {
        let mut current = Some(self);
        while let Some(page) = current {
            match page.content() {
                PageContent::BaseData(data) => return data.get(key),
                PageContent::DeltaData(data) => {
                    if let Some(value) = data.get(key) {
                        return value;
                    }
                }
            }
            current = page.next();
        }
        None
    }

    pub fn lookup_index(self, key: &[u8]) -> PageId {
        todo!()
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

    pub fn cas<'a>(
        &self,
        id: PageId,
        old: PageRef<'a>,
        new: PageRef<'a>,
    ) -> Result<(), PageRef<'a>> {
        let old = old.into_usize();
        let new = new.into_usize();
        self.table
            .cas(id, old, new)
            .map(|_| ())
            .map_err(|ptr| PageRef::from_usize(ptr))
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
        let ptr = page.into_usize();
        self.table.install(ptr, guard)
    }
}
