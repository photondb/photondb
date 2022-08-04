use super::{
    page::*,
    pagecache::{PageAddr, PageView},
    pagetable::PageTable,
};

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

pub struct DataIterChain<'a> {
    pub next: PageAddr,
    pub highest: Option<&'a [u8]>,
    pub children: Vec<DataPageIter<'a>>,
}

impl<'a> DataIterChain<'a> {
    pub fn new(next: PageAddr, highest: Option<&'a [u8]>, children: Vec<DataPageIter<'a>>) -> Self {
        Self {
            next,
            highest,
            children,
        }
    }

    pub fn iter(&mut self) -> MergingDataIter<'a, '_> {
        let mut merger = MergingIterBuilder::with_exact(self.children.len());
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
        let mut merger = MergingIterBuilder::with_exact(self.children.len());
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
