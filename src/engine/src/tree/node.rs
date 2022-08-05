use super::{page::*, pagestore::PageInfo, pagetable::PageTable};

pub const NULL_INDEX: Index = Index::with_id(PageTable::NAN);
pub const ROOT_INDEX: Index = Index::with_id(PageTable::MIN);

#[derive(Copy, Clone, Debug)]
pub struct Node<'a> {
    pub id: u64,
    pub view: PageView<'a>,
}

impl<'a> Node<'a> {
    pub fn new(id: u64, view: PageView<'a>) -> Self {
        Self { id, view }
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

pub struct DataIterChain<'a> {
    next: PageAddr,
    highest: Option<&'a [u8]>,
    children: Vec<DataPageIter<'a>>,
}

impl<'a> DataIterChain<'a> {
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

    pub fn iter(&mut self) -> MergingDataIter<'a, '_> {
        let mut merger = MergingIterBuilder::with_len(self.children.len());
        for iter in self.children.iter_mut() {
            merger.add(iter);
        }
        let iter = merger.build();
        MergingDataIter::new(iter, self.highest)
    }
}

pub struct MergingDataIter<'a, 'i> {
    iter: MergingIter<&'i mut DataPageIter<'a>>,
    highest: Option<&'a [u8]>,
}

impl<'a, 'i> MergingDataIter<'a, 'i> {
    pub fn new(iter: MergingIter<&'i mut DataPageIter<'a>>, highest: Option<&'a [u8]>) -> Self {
        Self { iter, highest }
    }
}

impl<'a, 'i> ForwardIter for MergingDataIter<'a, 'i> {
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

impl<'a, 'i> RewindableIter for MergingDataIter<'a, 'i> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

impl<'a, 'i> SeekableIter<Key<'_>> for MergingDataIter<'a, 'i> {
    fn seek(&mut self, target: &Key<'_>) {
        self.iter.seek(target);
    }
}

pub struct IndexIterChain<'a> {
    pub highest: Option<&'a [u8]>,
    pub children: Vec<IndexPageIter<'a>>,
}

impl<'a> IndexIterChain<'a> {
    pub fn new(highest: Option<&'a [u8]>, children: Vec<IndexPageIter<'a>>) -> Self {
        Self { highest, children }
    }

    pub fn iter(&mut self) -> MergingIndexIter<'a, '_> {
        let mut merger = MergingIterBuilder::with_len(self.children.len());
        for iter in self.children.iter_mut() {
            merger.add(iter);
        }
        let iter = merger.build();
        MergingIndexIter::new(iter, self.highest)
    }
}

pub struct MergingIndexIter<'a, 'i> {
    iter: MergingIter<&'i mut IndexPageIter<'a>>,
    highest: Option<&'a [u8]>,
    last_index: Index,
}

impl<'a, 'i> MergingIndexIter<'a, 'i> {
    pub fn new(iter: MergingIter<&'i mut IndexPageIter<'a>>, highest: Option<&'a [u8]>) -> Self {
        Self {
            iter,
            highest,
            last_index: NULL_INDEX,
        }
    }
}

impl<'a, 'i> ForwardIter for MergingIndexIter<'a, 'i> {
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

impl<'a, 'i> RewindableIter for MergingIndexIter<'a, 'i> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

impl<'a, 'i> SeekableIter<[u8]> for MergingIndexIter<'a, 'i> {
    fn seek(&mut self, target: &[u8]) {
        self.iter.seek(target);
    }
}

pub struct DataNodeIter<'a> {
    //iter: MergingIter<DataPageIter<'a>>,
    iter: MergingIter2<DataPageIter<'a>>,
    next: PageAddr,
    highest: Option<&'a [u8]>,
}

impl<'a> DataNodeIter<'a> {
    pub fn new(next: PageAddr, highest: Option<&'a [u8]>, children: Vec<DataPageIter<'a>>) -> Self {
        //let iter = MergingIterBuilder::with(children).build();
        let iter = MergingIter2::new(children);
        Self {
            iter,
            next,
            highest,
        }
    }

    pub const fn next(&self) -> PageAddr {
        self.next
    }
}

impl<'a> ForwardIter for DataNodeIter<'a> {
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

impl<'a> RewindableIter for DataNodeIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

impl<'a> SeekableIter<Key<'_>> for DataNodeIter<'a> {
    fn seek(&mut self, target: &Key<'_>) {
        self.iter.seek(target);
    }
}
