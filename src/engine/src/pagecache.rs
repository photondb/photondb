use std::sync::{
    atomic::{AtomicPtr, AtomicUsize, Ordering},
    Arc,
};

use crossbeam_epoch::Guard;

use crate::{DeltaPage, PageContent, PageId, PageTable};

#[derive(Debug)]
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
        header.len += 1;
        header.next = next.into_usize();
        Self { header, content }
    }
}

#[derive(Clone, Debug)]
pub struct PageHeader {
    len: usize,
    next: usize,
    is_data: bool,
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
}

impl PageHeader {
    pub fn new() -> Self {
        Self {
            len: 1,
            next: 0,
            is_data: true,
            lower_bound: Vec::new(),
            upper_bound: Vec::new(),
        }
    }

    pub fn covers(&self, key: &[u8]) -> bool {
        key >= &self.lower_bound && (key < &self.upper_bound || self.upper_bound.is_empty())
    }

    pub fn split_at(&mut self, key: &[u8]) -> PageHeader {
        assert!(self.covers(key));
        let mut right = self.clone();
        self.upper_bound = key.to_vec();
        right.lower_bound = key.to_vec();
        right
    }
}

#[derive(Copy, Clone, Debug)]
pub struct PageRef<'a>(&'a Page);

impl<'a> PageRef<'a> {
    fn from_usize(ptr: usize) -> Self {
        Self(unsafe { &*(ptr as *const Page) })
    }

    fn into_usize(self) -> usize {
        self.0 as *const Page as _
    }

    pub fn len(self) -> usize {
        self.0.header.len
    }

    pub fn next(self) -> Option<PageRef<'a>> {
        let next = self.0.header.next;
        if next == 0 {
            None
        } else {
            Some(PageRef::from_usize(next))
        }
    }

    pub fn is_data(self) -> bool {
        self.0.header.is_data
    }

    pub fn covers(self, key: &[u8]) -> bool {
        self.0.header.covers(key)
    }

    pub fn lower_bound(self) -> &'a [u8] {
        &self.0.header.lower_bound
    }

    pub fn upper_bound(self) -> &'a [u8] {
        &self.0.header.upper_bound
    }

    pub fn content(self) -> &'a PageContent {
        &self.0.content
    }

    pub fn consolidate(self, split_size: usize) -> (Page, Option<Page>) {
        let mut page = self;
        let mut delta = DeltaPage::new();
        loop {
            match page.content() {
                PageContent::Base(base) => {
                    let mut base = base.clone();
                    base.merge(delta);
                    let header = page.0.header.clone();
                    let mut right_page = None;
                    if base.size() >= split_size {
                        if let Some((split_key, right_base)) = base.split() {
                            let right_header = header.split_at(&split_key);
                            right_page =
                                Some(Page::new(right_header, PageContent::Base(right_base)));
                        }
                    }
                    let left_page = Page::new(header, PageContent::Base(base));
                    return (left_page, right_page);
                }
                PageContent::Delta(data) => {
                    delta.merge(data.clone());
                }
                PageContent::Split(_) => {}
            }
            page = page.next().unwrap();
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PageId(usize);

impl PageId {
    pub const fn min() -> Self {
        Self(MIN_ID)
    }

    pub const fn max() -> Self {
        Self(MAX_ID)
    }
}

pub struct PageCache {
    map: PageTable,
    free: AtomicUsize,
}

impl PageCache {
    fn alloc_id(&self) -> Option<PageId> {
        let mut id = self.free.load(Ordering::Acquire);
        while id != PageTable::nan() {
            let next = self.map[id].load(Ordering::Acquire);
            match self
                .free
                .compare_exchange(id, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => id = actual,
            }
        }
        if id != PageTable::nan() {
            Some(id)
        } else {
            self.map.alloc().map(PageId)
        }
    }

    fn dealloc_id(&self, id: usize) {
        let head = self.map[id];
        let mut next = self.free.load(Ordering::Acquire);
        loop {
            head.store(next, Ordering::Release);
            match self
                .free
                .compare_exchange(next, id, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => next = actual,
            }
        }
    }

    pub fn new() -> Self {
        Self {
            map: PageTable::new(),
            free: AtomicUsize::new(PageTable::nan()),
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

    pub fn replace<'a>(
        &self,
        id: PageId,
        old: PageRef<'a>,
        new: PageRef<'a>,
        guard: &'a Guard,
    ) -> Result<(), PageRef<'a>> {
        self.cas(id, old, new).map(|_| self.dealloc(old, guard))
    }

    pub fn alloc(&self, page: Page) -> PageRef<'static> {
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

    pub fn uninstall(&self, id: PageId, guard: &Guard) {
        let page = self.get(id, guard);
        self.dealloc(page, guard);
        self.table.uninstall(id, guard)
    }
}
