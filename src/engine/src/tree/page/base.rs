use std::{alloc::Layout, iter::Iterator, ops::Deref};

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
    Data = 0,
    Index = 16,
}

impl PageKind {
    pub fn is_data(self) -> bool {
        self < Self::Index
    }
}

#[repr(C)]
pub struct PageHeader {
    pub ver: u64,
    pub next: u64,
}

pub trait PageLayout {
    fn layout(&self) -> Layout;
}

pub struct PageBuf(Box<[u8]>);

impl PageBuf {
    pub fn ver(&self) -> u64 {
        self.as_ref().ver()
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

pub trait PageIter: Iterator {
    type Item;

    fn seek(&mut self, key: &[u8]);
}

pub struct MergeIter<I>
where
    I: PageIter,
{
    children: Vec<I>,
}

impl<I> PageIter for MergeIter<I>
where
    I: PageIter,
{
    type Item = <I as PageIter>::Item;

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }
}

impl<I> Iterator for MergeIter<I>
where
    I: PageIter,
{
    type Item = <I as PageIter>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct MergeIterBuilder<I>
where
    I: PageIter,
{
    children: Vec<I>,
}

impl<I> Default for MergeIterBuilder<I>
where
    I: PageIter,
{
    fn default() -> Self {
        Self {
            children: Vec::new(),
        }
    }
}

impl<I> MergeIterBuilder<I>
where
    I: PageIter,
{
    pub fn add(&mut self, child: I) {
        self.children.push(child);
    }

    pub fn build(self) -> MergeIter<I> {
        MergeIter {
            children: self.children,
        }
    }
}
