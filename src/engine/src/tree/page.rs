pub enum PagePtr {
    Mem(u64),
    Disk(u64),
}

const MEM_DISK_MASK: u64 = 1 << 63;

impl From<u64> for PagePtr {
    fn from(addr: u64) -> Self {
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

#[derive(Copy, Clone, Debug)]
pub struct PageRef<'a>(&'a [u8]);

impl<'a> PageRef<'a> {
    pub fn ver(self) -> u64 {
        todo!()
    }

    pub fn len(self) -> u8 {
        todo!()
    }

    pub fn kind(self) -> PageKind {
        todo!()
    }

    pub fn next(self) -> Option<PagePtr> {
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

impl PageBuf {}

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

pub struct BaseData<'a>(&'a [u8]);

impl<'a> BaseData<'a> {
    pub fn get(self, key: &[u8]) -> Option<Value<'a>> {
        todo!()
    }
}

impl<'a> From<PageRef<'a>> for BaseData<'a> {
    fn from(page: PageRef<'a>) -> Self {
        todo!()
    }
}

pub struct DeltaDataBuf {}

impl DeltaDataBuf {
    pub fn add(&mut self, record: &Record) {
        todo!()
    }
}

pub struct DeltaDataRef<'a>(&'a [u8]);

impl<'a> DeltaDataRef<'a> {
    pub fn get(self, key: &[u8]) -> Option<Value<'a>> {
        todo!()
    }
}

impl<'a> From<PageRef<'a>> for DeltaDataRef<'a> {
    fn from(page: PageRef<'a>) -> Self {
        todo!()
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

pub struct DeltaDataLayout {}

impl Default for DeltaDataLayout {
    fn default() -> Self {
        Self {}
    }
}

impl DeltaDataLayout {
    pub fn add(&mut self, record: &Record) {
        todo!()
    }
}

impl PageLayout for DeltaDataLayout {}
