use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crossbeam_epoch::Guard;

use crate::PageTable;

#[derive(Debug)]
pub struct Page {
    header: PageHeader,
    content: PageContent,
}

impl Page {
    pub fn new(header: PageHeader, content: PageContent) -> Self {
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

    pub fn with_next(next: PageRef<'_>) -> Self {
        Self {
            len: next.len() + 1,
            next: next.into_usize(),
            is_data: next.is_data(),
            lower_bound: next.lower_bound().to_owned(),
            upper_bound: next.upper_bound().to_owned(),
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key >= &self.lower_bound && (key < &self.upper_bound || self.upper_bound.is_empty())
    }

    pub fn split_at(&mut self, key: &[u8]) -> PageHeader {
        assert!(self.contains(key));
        let right = PageHeader {
            len: 1,
            next: 0,
            is_data: self.is_data,
            lower_bound: key.to_vec(),
            upper_bound: self.upper_bound.clone(),
        };
        self.upper_bound = key.to_vec();
        right
    }
}

#[derive(Debug)]
pub enum PageContent {
    Base(BasePage),
    Delta(DeltaPage),
    Split(SplitPage),
}

#[derive(Clone, Debug)]
pub struct BasePage {
    size: usize,
    records: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BasePage {
    pub fn new() -> Self {
        Self {
            size: 0,
            records: BTreeMap::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.records.get(key).map(|v| v.as_slice())
    }

    pub fn merge(&mut self, delta: DeltaPage) {
        for (key, value) in delta.records {
            if let Some(value) = value {
                self.size += key.len() + value.len();
                if let Some(old_value) = self.records.insert(key, value) {
                    self.size -= old_value.len();
                }
            } else {
                if let Some(old_value) = self.records.remove(&key) {
                    self.size -= key.len() + old_value.len();
                }
            }
        }
    }

    pub fn split(&mut self) -> Option<(Vec<u8>, BasePage)> {
        if let Some(key) = self.records.keys().nth(self.records.len() / 2).cloned() {
            let mut right = BasePage::new();
            right.records = self.records.split_off(&key);
            right.size = right
                .records
                .iter()
                .fold(0, |acc, (k, v)| acc + k.len() + v.len());
            Some((key, right))
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeltaPage {
    records: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl DeltaPage {
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        self.records
            .get(key)
            .map(|v| v.as_ref().map(|v| v.as_slice()))
    }

    pub fn add(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.records.insert(key, value);
    }

    pub fn merge(&mut self, other: DeltaPage) {
        for (key, value) in other.records {
            self.records.entry(key).or_insert(value);
        }
    }
}

#[derive(Debug)]
pub struct SplitPage {
    pub key: Vec<u8>,
    pub right: PageId,
}

/*
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
*/

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

    pub fn contains(self, key: &[u8]) -> bool {
        self.0.header.contains(key)
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

    pub fn get<'a>(&self, id: PageId, _: &'a Guard) -> PageRef<'a> {
        let id = id.into_usize();
        let ptr = self.table.get(id);
        PageRef::from_usize(ptr)
    }

    pub fn update<'a>(
        &self,
        id: PageId,
        old: PageRef<'a>,
        new: PageRef<'a>,
        _: &'a Guard,
    ) -> Result<(), PageRef<'a>> {
        let id = id.into_usize();
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
        self.update(id, old, new, guard)?;
        let mut cursor = Some(old);
        while let Some(page) = cursor {
            cursor = page.next();
            self.dealloc(page, guard);
        }
        Ok(())
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
        if let Some(id) = self.table.alloc() {
            self.table.set(id, page.into_usize());
            Some(PageId::from_usize(id))
        } else {
            None
        }
    }

    pub fn uninstall<'a>(&self, id: PageId, guard: &'a Guard) {
        let page = self.get(id, guard);
        self.dealloc(page, guard);
        let id = id.into_usize();
        let table = self.table.clone();
        guard.defer(move || {
            table.dealloc(id);
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
