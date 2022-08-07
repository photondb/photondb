use super::{page::*, pagestore::PageInfo, pagetable::PageTable};

pub const NULL_INDEX: Index = Index::with_id(PageTable::NAN);
pub const ROOT_INDEX: Index = Index::with_id(PageTable::MIN);

#[derive(Copy, Clone, Debug)]
pub struct Node<'a> {
    pub id: u64,
    pub page: PageView<'a>,
}

impl<'a> Node<'a> {
    pub fn new(id: u64, page: PageView<'a>) -> Self {
        Self { id, page }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PageAddr {
    Mem(u64),
    Disk(u64),
}

const MEM_DISK_MASK: u64 = 1 << 63;

impl From<u64> for PageAddr {
    fn from(addr: u64) -> Self {
        if addr & MEM_DISK_MASK == 0 {
            Self::Mem(addr)
        } else {
            Self::Disk(addr & !MEM_DISK_MASK)
        }
    }
}

impl From<PageAddr> for u64 {
    fn from(addr: PageAddr) -> u64 {
        match addr {
            PageAddr::Mem(addr) => addr,
            PageAddr::Disk(addr) => addr | MEM_DISK_MASK,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum PageView<'a> {
    Mem(PageRef<'a>),
    Disk(PageInfo, u64),
}

impl PageView<'_> {
    pub fn ver(&self) -> u64 {
        match self {
            Self::Mem(page) => page.ver(),
            Self::Disk(info, _) => info.ver,
        }
    }

    pub fn rank(&self) -> u8 {
        match self {
            Self::Mem(page) => page.rank(),
            Self::Disk(info, _) => info.rank,
        }
    }

    pub fn is_data(&self) -> bool {
        match self {
            Self::Mem(page) => page.is_data(),
            Self::Disk(info, _) => info.is_data,
        }
    }

    pub fn as_addr(&self) -> PageAddr {
        match *self {
            Self::Mem(page) => PageAddr::Mem(page.into()),
            Self::Disk(_, addr) => PageAddr::Disk(addr),
        }
    }
}

impl<'a, T> From<T> for PageView<'a>
where
    T: Into<PageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::Mem(page.into())
    }
}

pub struct DataNodeView<'a> {
    next: PageAddr,
    highest: Option<&'a [u8]>,
    children: Vec<DataPageIter<'a>>,
}

impl<'a> DataNodeView<'a> {
    pub fn new(next: PageAddr, highest: Option<&'a [u8]>, children: Vec<DataPageIter<'a>>) -> Self {
        Self {
            next,
            highest,
            children,
        }
    }

    pub const fn next(&self) -> PageAddr {
        self.next
    }

    pub fn iter(&mut self) -> DataIter<'a, &mut DataPageIter<'a>> {
        let mut merger = MergingIterBuilder::with_len(self.children.len());
        for child in self.children.iter_mut() {
            merger.add(child);
        }
        let iter = merger.build();
        DataIter::new(iter, self.highest)
    }
}

pub struct DataIter<'a, T>
where
    T: ForwardIter<Item = DataItem<'a>>,
{
    iter: MergingIter<T>,
    highest: Option<&'a [u8]>,
}

impl<'a, T> DataIter<'a, T>
where
    T: ForwardIter<Item = DataItem<'a>>,
{
    pub fn new(iter: MergingIter<T>, highest: Option<&'a [u8]>) -> Self {
        Self { iter, highest }
    }
}

impl<'a, T> ForwardIter for DataIter<'a, T>
where
    T: ForwardIter<Item = DataItem<'a>>,
{
    type Item = DataItem<'a>;

    fn last(&self) -> Option<&Self::Item> {
        self.iter.last()
    }

    fn next(&mut self) -> Option<&Self::Item> {
        if let Some((key, _)) = self.iter.next() {
            if let Some(highest) = self.highest {
                if key.raw >= highest {
                    self.iter.skip_all();
                }
            }
        }
        self.iter.last()
    }

    fn skip_all(&mut self) {
        self.iter.skip_all()
    }
}

impl<'a, T> SeekableIter<Key<'_>> for DataIter<'a, T>
where
    for<'k> T: SeekableIter<Key<'k>, Item = DataItem<'a>>,
{
    fn seek(&mut self, target: &Key<'_>) {
        self.iter.seek(target);
    }
}

impl<'a, T> RewindableIter for DataIter<'a, T>
where
    T: RewindableIter<Item = DataItem<'a>>,
{
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

pub struct IndexNodeView<'a> {
    pub highest: Option<&'a [u8]>,
    pub children: Vec<IndexPageIter<'a>>,
}

impl<'a> IndexNodeView<'a> {
    pub fn new(highest: Option<&'a [u8]>, children: Vec<IndexPageIter<'a>>) -> Self {
        Self { highest, children }
    }

    pub fn iter(&mut self) -> IndexIter<'a, &mut IndexPageIter<'a>> {
        let mut merger = MergingIterBuilder::with_len(self.children.len());
        for iter in self.children.iter_mut() {
            merger.add(iter);
        }
        let iter = merger.build();
        IndexIter::new(iter, self.highest)
    }
}

pub struct IndexIter<'a, T>
where
    T: ForwardIter<Item = IndexItem<'a>>,
{
    iter: MergingIter<T>,
    highest: Option<&'a [u8]>,
    last_index: Index,
}

impl<'a, T> IndexIter<'a, T>
where
    T: ForwardIter<Item = IndexItem<'a>>,
{
    pub fn new(iter: MergingIter<T>, highest: Option<&'a [u8]>) -> Self {
        Self {
            iter,
            highest,
            last_index: NULL_INDEX,
        }
    }
}

impl<'a, T> ForwardIter for IndexIter<'a, T>
where
    T: ForwardIter<Item = IndexItem<'a>>,
{
    type Item = IndexItem<'a>;

    fn last(&self) -> Option<&Self::Item> {
        self.iter.last()
    }

    fn next(&mut self) -> Option<&Self::Item> {
        while let Some(&(key, index)) = self.iter.next() {
            if let Some(highest) = self.highest {
                if key >= highest {
                    self.iter.skip_all();
                    break;
                }
            }
            if index == NULL_INDEX || index.id == self.last_index.id {
                continue;
            }
            self.last_index = index;
            break;
        }
        self.iter.last()
    }

    fn skip_all(&mut self) {
        self.iter.skip_all()
    }
}

impl<'a, T> SeekableIter<[u8]> for IndexIter<'a, T>
where
    for<'k> T: SeekableIter<[u8], Item = IndexItem<'a>>,
{
    fn seek(&mut self, target: &[u8]) {
        self.iter.seek(target);
    }
}

impl<'a, T> RewindableIter for IndexIter<'a, T>
where
    T: RewindableIter<Item = IndexItem<'a>>,
{
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}
