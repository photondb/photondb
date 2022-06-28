use crate::PageTable;

use std::collections::BTreeMap;

pub struct Page {
    header: PageHeader,
    content: PageContent,
}

impl Page {
    pub fn new(header: PageHeader, content: PageContent) -> Self {
        Self { header, content }
    }
}

#[derive(Clone)]
pub struct PageHeader {
    next: u64,
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
}

impl PageHeader {
    pub fn covers(&self, key: &[u8]) -> bool {
        key >= &self.lower_bound && (key <= &self.upper_bound || self.upper_bound.is_empty())
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            next: 0,
            lower_bound: vec![],
            upper_bound: vec![],
        }
    }
}

pub enum PageContent {
    BaseData(BaseData),
    DeltaData(DeltaData),
}

impl PageContent {
    pub fn is_data(&self) -> bool {
        match self {
            PageContent::BaseData(_) | PageContent::DeltaData(_) => true,
        }
    }
}

#[derive(Copy, Clone)]
pub struct PagePtr<'a>(&'a Page);

impl<'a> PagePtr<'a> {
    pub fn new(page: &'a Page) -> Self {
        Self(page)
    }

    pub fn next(self) -> Option<PagePtr<'a>> {
        if self.0.header.next == 0 {
            None
        } else {
            Some(self.0.header.next.into())
        }
    }

    pub fn set_next(self, next: Option<PagePtr<'a>>) {
        self.0.header.next = next.map(|p| p.into()).unwrap_or(0);
    }

    pub fn covers(self, key: &[u8]) -> bool {
        self.0.header.covers(key)
    }

    pub fn is_data(self) -> bool {
        self.0.content.is_data()
    }

    pub fn content(self) -> &'a PageContent {
        &self.0.content
    }
}

impl<'a> From<u64> for PagePtr<'a> {
    fn from(ptr: u64) -> Self {
        Self(unsafe { &*(ptr as *const Page) })
    }
}

impl<'a> Into<u64> for PagePtr<'a> {
    fn into(self) -> u64 {
        self.0 as *const Page as u64
    }
}

pub struct BaseData {
    records: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BaseData {
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.records.get(key).map(|v| v.as_slice())
    }
}

pub struct DeltaData {
    records: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl DeltaData {
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
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PageId(u64);

impl PageId {
    pub const fn zero() -> PageId {
        PageId(0)
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

    pub fn get<'a>(&self, id: PageId) -> PagePtr<'a> {
        self.table.get(id.0).into()
    }

    pub fn cas<'a>(&self, id: PageId, old: PagePtr<'a>, new: PagePtr<'a>) -> Option<PagePtr<'a>> {
        self.table
            .cas(id.0, old.into(), new.into())
            .map(|ptr| ptr.into())
    }

    pub fn install<'a>(&self, new: PagePtr<'a>) -> PageId {
        PageId(self.table.install(new.into()))
    }

    pub fn uninstall(&self, id: PageId) {
        todo!();
    }
}
