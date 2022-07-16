use super::iter::PageIter;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PagePtr {
    Mem(u64),
    Disk(u64),
}

const MEM_DISK_MASK: u64 = 1 << 63;

impl From<u64> for PagePtr {
    fn from(addr: u64) -> Self {
        assert!(addr != 0);
        if addr & MEM_DISK_MASK == 0 {
            Self::Mem(addr)
        } else {
            Self::Disk(addr & !MEM_DISK_MASK)
        }
    }
}

impl<'a> Into<u64> for PagePtr {
    fn into(self) -> u64 {
        match self {
            Self::Mem(addr) => addr,
            Self::Disk(addr) => addr | MEM_DISK_MASK,
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageKind {
    BaseData = 0,
    DeltaData = 1,
    BaseIndex = 32,
    DeltaIndex = 33,
}

impl PageKind {
    pub fn is_data(self) -> bool {
        self < Self::BaseIndex
    }
}

pub trait PageLayout {}

#[derive(Copy, Clone, Debug)]
pub struct PageRef<'a>(&'a [u8]);

impl<'a> PageRef<'a> {
    pub fn ver(&self) -> u64 {
        todo!()
    }

    pub fn len(&self) -> u8 {
        todo!()
    }

    pub fn kind(&self) -> PageKind {
        todo!()
    }

    pub fn next(&self) -> Option<PagePtr> {
        todo!()
    }
}

impl<'a> From<u64> for PageRef<'a> {
    fn from(addr: u64) -> Self {
        todo!()
    }
}

impl<'a> Into<u64> for PageRef<'a> {
    fn into(self) -> u64 {
        todo!()
    }
}

pub struct PageBuf(Box<[u8]>);

impl PageBuf {
    pub fn as_ptr(&self) -> PagePtr {
        todo!()
    }
}

impl From<Box<[u8]>> for PageBuf {
    fn from(buf: Box<[u8]>) -> Self {
        Self(buf)
    }
}

pub enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

pub struct Record<'a> {
    pub lsn: u64,
    pub key: &'a [u8],
    pub value: Value<'a>,
}

impl<'a> Record<'a> {
    pub fn put(lsn: u64, key: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            lsn,
            key,
            value: Value::Put(value),
        }
    }

    pub fn delete(lsn: u64, key: &'a [u8]) -> Self {
        Self {
            lsn,
            key,
            value: Value::Delete,
        }
    }
}

pub struct DataPageRef<'a>(&'a [u8]);

impl<'a> DataPageRef<'a> {
    pub fn get(self, key: &[u8]) -> Option<Value<'a>> {
        todo!()
    }

    pub fn iter(self) -> DataPageIter<'a> {
        todo!()
    }
}

impl<'a> From<PageRef<'a>> for DataPageRef<'a> {
    fn from(page: PageRef<'a>) -> Self {
        todo!()
    }
}

pub struct DataPageBuf(Box<[u8]>);

impl DataPageBuf {
    pub fn add(&mut self, record: &Record) {
        todo!()
    }

    pub fn as_ptr(&self) -> PagePtr {
        todo!()
    }
}

impl From<PageBuf> for DataPageBuf {
    fn from(buf: PageBuf) -> Self {
        Self(buf.0)
    }
}

impl From<DataPageBuf> for PageBuf {
    fn from(buf: DataPageBuf) -> Self {
        todo!()
    }
}

pub struct DataPageLayout {}

impl Default for DataPageLayout {
    fn default() -> Self {
        Self {}
    }
}

impl DataPageLayout {
    pub fn add(&mut self, record: &Record) {
        todo!()
    }
}

impl PageLayout for DataPageLayout {}

pub struct DataPageIter<'a>(&'a [u8]);

impl<'a> PageIter for DataPageIter<'a> {
    type Item = Record<'a>;

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }
}

impl<'a> Iterator for DataPageIter<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
