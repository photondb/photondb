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

pub struct PageBuf(Box<[u8]>);

impl PageBuf {
    pub fn ver(&self) -> u64 {
        todo!()
    }

    pub fn set_ver(&mut self, ver: u64) {
        todo!()
    }

    pub fn len(&self) -> u8 {
        todo!()
    }

    pub fn set_len(&mut self, len: u8) {
        todo!()
    }

    pub fn size(&self) -> usize {
        todo!()
    }

    pub fn set_next(&mut self, next: PagePtr) {
        todo!()
    }

    pub fn as_ptr(&self) -> PagePtr {
        self.as_ref().into()
    }

    pub fn as_ref(&self) -> PageRef<'_> {
        PageRef(self.0.as_ref())
    }
}

impl From<Box<[u8]>> for PageBuf {
    fn from(buf: Box<[u8]>) -> Self {
        Self(buf)
    }
}

impl Into<Box<[u8]>> for PageBuf {
    fn into(self) -> Box<[u8]> {
        self.0
    }
}

#[derive(Copy, Clone, Debug)]
pub struct PageRef<'a>(&'a [u8]);

impl<'a> PageRef<'a> {
    pub fn ver(&self) -> u64 {
        todo!()
    }

    pub fn len(&self) -> u8 {
        todo!()
    }

    pub fn size(&self) -> usize {
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

impl<'a> Into<PagePtr> for PageRef<'a> {
    fn into(self) -> PagePtr {
        PagePtr::Mem(self.into())
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageKind {
    Data = 0,
    Index = 16,
}

impl PageKind {
    pub fn is_data(self) -> bool {
        self < Self::Index
    }
}
