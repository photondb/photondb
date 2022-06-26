use crate::PageTable;

use std::collections::BTreeMap;

#[derive(Copy, Clone)]
pub enum Page<'a> {
    BaseData(&'a BaseData),
    DeltaData(&'a DeltaData),
}

impl Page<'_> {
    pub fn as_ptr(self) -> PagePtr {
        match self {
            Page::BaseData(page) => {
                PagePtr::new(PageKind::BaseData, page as *const BaseData as u64)
            }
            Page::DeltaData(page) => {
                PagePtr::new(PageKind::DeltaData, page as *const DeltaData as u64)
            }
        }
    }

    pub fn is_data(&self) -> bool {
        match self {
            Page::BaseData(_) | Page::DeltaData(_) => true,
        }
    }
}

pub struct BaseData {
    records: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BaseData {
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.records.get(key).map(|v| v.as_slice())
    }
}

impl Default for BaseData {
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }
}

pub struct DeltaData {
    records: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    next: PagePtr,
}

impl DeltaData {
    pub fn new(next: PagePtr) -> Self {
        Self {
            records: BTreeMap::new(),
            next,
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

    pub fn next(&self) -> PagePtr {
        self.next
    }

    pub fn set_next(&mut self, next: PagePtr) {
        self.next = next;
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PageId(u64);

impl PageId {
    pub const fn zero() -> PageId {
        PageId(0)
    }
}

// Layout: mem/disk (1 bit) | page_kind (7 bits) | reserved (8 bits) | page_addr (48 bits)
#[derive(Copy, Clone)]
pub struct PagePtr(u64);

impl PagePtr {
    pub fn new(kind: PageKind, addr: u64) -> Self {
        Self((kind as u64) << 56 | addr)
    }

    pub fn kind(self) -> PageKind {
        (((self.0 >> 56) & 0x7F) as u8).into()
    }

    pub fn addr(self) -> u64 {
        self.0 & 0x0000FFFFFFFFFFFF
    }

    pub fn deref<'a>(self) -> Page<'a> {
        match self.kind() {
            PageKind::BaseData => Page::BaseData(unsafe { &*(self.addr() as *const BaseData) }),
            PageKind::DeltaData => Page::DeltaData(unsafe { &*(self.addr() as *const DeltaData) }),
        }
    }
}

pub enum PageKind {
    BaseData = 0,
    DeltaData = 1,
}

impl From<u8> for PageKind {
    fn from(kind: u8) -> Self {
        match kind {
            0 => PageKind::BaseData,
            1 => PageKind::DeltaData,
            _ => panic!("invalid page kind"),
        }
    }
}

impl Into<u8> for PageKind {
    fn into(self) -> u8 {
        match self {
            PageKind::BaseData => 0,
            PageKind::DeltaData => 1,
        }
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

    pub fn get(&self, id: PageId) -> PagePtr {
        PagePtr(self.table.get(id.0))
    }

    pub fn cas(&self, id: PageId, old: PagePtr, new: PagePtr) -> Option<PagePtr> {
        self.table.cas(id.0, old.0, new.0).map(|ptr| PagePtr(ptr))
    }

    pub fn install(&self, new: PagePtr) -> PageId {
        PageId(self.table.install(new.0))
    }

    pub fn uninstall(&self, id: PageId) {
        todo!();
    }
}
