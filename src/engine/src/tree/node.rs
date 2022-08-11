use super::{page::*, pagestore::PageInfo, pagetable::PageTable};

pub const NULL_INDEX: Index = Index::new(PageTable::NAN, 0);
pub const ROOT_INDEX: Index = Index::new(PageTable::MIN, 0);

#[derive(Clone, Debug)]
pub struct Node<'a> {
    pub id: u64,
    pub view: PageView<'a>,
    pub range: Range<'a>,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Range<'a> {
    pub start: &'a [u8],
    pub end: Option<&'a [u8]>,
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

    pub fn len(&self) -> u8 {
        match self {
            Self::Mem(page) => page.len(),
            Self::Disk(info, _) => info.len,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            Self::Mem(page) => page.is_leaf(),
            Self::Disk(info, _) => info.is_leaf,
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

pub struct DataIter<'a, T>
where
    T: ForwardIter<Item = DataItem<'a>>,
{
    iter: T,
    limit: Option<&'a [u8]>,
}

impl<'a, T> DataIter<'a, T>
where
    T: ForwardIter<Item = DataItem<'a>>,
{
    pub fn new(iter: T, limit: Option<&'a [u8]>) -> Self {
        Self { iter, limit }
    }

    fn find_next(&mut self) {
        if let Some((key, _)) = self.iter.current() {
            if let Some(limit) = self.limit {
                if key.raw >= limit {
                    self.iter.skip_all();
                }
            }
        }
    }
}

impl<'a, T> ForwardIter for DataIter<'a, T>
where
    T: ForwardIter<Item = DataItem<'a>>,
{
    type Item = DataItem<'a>;

    fn current(&self) -> Option<&Self::Item> {
        self.iter.current()
    }

    fn rewind(&mut self) {
        self.iter.rewind();
        self.find_next();
    }

    fn next(&mut self) {
        self.iter.next();
        self.find_next();
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
        self.find_next();
    }
}

pub struct IndexIter<'a, T>
where
    T: ForwardIter<Item = IndexItem<'a>>,
{
    iter: T,
    limit: Option<&'a [u8]>,
    last_index: Index,
}

impl<'a, T> IndexIter<'a, T>
where
    T: ForwardIter<Item = IndexItem<'a>>,
{
    pub fn new(iter: T, limit: Option<&'a [u8]>) -> Self {
        Self {
            iter,
            limit,
            last_index: NULL_INDEX,
        }
    }

    fn reset(&mut self) {
        self.last_index = NULL_INDEX;
    }

    fn find_next(&mut self) {
        while let Some(&(key, index)) = self.iter.current() {
            if let Some(limit) = self.limit {
                if key >= limit {
                    self.iter.skip_all();
                    break;
                }
            }
            if index != NULL_INDEX && index.id != self.last_index.id {
                self.last_index = index;
                break;
            }
            self.iter.next();
        }
    }
}

impl<'a, T> ForwardIter for IndexIter<'a, T>
where
    T: ForwardIter<Item = IndexItem<'a>>,
{
    type Item = IndexItem<'a>;

    fn current(&self) -> Option<&Self::Item> {
        self.iter.current()
    }

    fn rewind(&mut self) {
        self.iter.rewind();
        self.reset();
        self.find_next();
    }

    fn next(&mut self) {
        self.iter.next();
        self.find_next();
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
        self.reset();
        self.find_next();
    }
}

pub type DataNodeIter<'a, 'b> = DataIter<'a, MergingIter<OrderedIter<&'b mut DataPageIter<'a>>>>;
pub type IndexNodeIter<'a, 'b> = IndexIter<'a, MergingIter<OrderedIter<&'b mut IndexPageIter<'a>>>>;

pub struct ConsolidateDataIter<'a, 'b> {
    iter: DataNodeIter<'a, 'b>,
    base: Option<PageRef<'a>>,
    min_lsn: u64,
    last_raw: &'a [u8],
}

impl<'a, 'b> ConsolidateDataIter<'a, 'b> {
    pub fn new(iter: DataNodeIter<'a, 'b>, base: Option<PageRef<'a>>, min_lsn: u64) -> Self {
        Self {
            iter,
            base,
            min_lsn,
            last_raw: &[],
        }
    }

    pub fn base(&self) -> Option<PageRef<'a>> {
        self.base
    }

    fn reset(&mut self) {
        self.last_raw = &[];
    }

    fn find_next(&mut self) {
        while let Some((k, v)) = self.iter.current() {
            if k.raw != self.last_raw {
                self.last_raw = k.raw;
                match v {
                    Value::Put(_) => return,
                    Value::Delete => {
                        // We need to keep this deletion to mask older versions.
                        if k.lsn >= self.min_lsn {
                            return;
                        }
                    }
                }
            } else if k.lsn >= self.min_lsn {
                return;
            }
            self.iter.next();
        }
    }
}

impl<'a, 'b> ForwardIter for ConsolidateDataIter<'a, 'b> {
    type Item = DataItem<'a>;

    fn current(&self) -> Option<&Self::Item> {
        self.iter.current()
    }

    fn rewind(&mut self) {
        self.iter.rewind();
        self.reset();
        self.find_next();
    }

    fn next(&mut self) {
        self.iter.next();
        self.find_next();
    }

    fn skip_all(&mut self) {
        self.iter.skip_all()
    }
}

impl<'a, 'b> SeekableIter<Key<'_>> for ConsolidateDataIter<'a, 'b> {
    fn seek(&mut self, target: &Key<'_>) {
        self.iter.seek(target);
        self.reset();
        self.find_next();
    }
}
