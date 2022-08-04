use super::{page::*, pagecache::PageView, pagetable::PageTable};

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

pub struct DataNodeIter<'a> {
    highest: Option<&'a [u8]>,
    children: Vec<DataPageIter<'a>>,
}

impl<'a> DataNodeIter<'a> {
    pub fn iter(&mut self) -> DataIter<'a, '_> {
        let mut merger = MergingIterBuilder::with_exact(self.children.len());
        for iter in self.children.iter_mut() {
            merger.add(iter);
        }
        let iter = merger.build();
        DataIter::new(iter, self.highest.clone())
    }
}

pub struct DataIter<'a, 'i> {
    iter: MergingIter<&'i mut DataPageIter<'a>>,
    highest: Option<&'a [u8]>,
}

impl<'a, 'i> DataIter<'a, 'i> {
    pub fn new(iter: MergingIter<&'i mut DataPageIter<'a>>, highest: Option<&'a [u8]>) -> Self {
        Self { iter, highest }
    }
}

impl<'a, 'i> ForwardIter for DataIter<'a, 'i> {
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

impl<'a, 'i> RewindableIter for DataIter<'a, 'i> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

impl<'a, 'i> SeekableIter<Key<'_>> for DataIter<'a, 'i> {
    fn seek(&mut self, target: &Key<'_>) {
        self.iter.seek(target);
    }
}

pub struct IndexNodeIter<'a> {
    highest: Option<&'a [u8]>,
    children: Vec<IndexPageIter<'a>>,
}

impl<'a> IndexNodeIter<'a> {
    pub fn iter(&mut self) -> IndexIter<'a, '_> {
        let mut merger = MergingIterBuilder::with_exact(self.children.len());
        for iter in self.children.iter_mut() {
            merger.add(iter);
        }
        let iter = merger.build();
        IndexIter::new(iter, self.highest.clone())
    }
}

pub struct IndexIter<'a, 'i> {
    iter: MergingIter<&'i mut IndexPageIter<'a>>,
    highest: Option<&'a [u8]>,
    last_index: Index,
}

impl<'a, 'i> IndexIter<'a, 'i> {
    pub fn new(iter: MergingIter<&'i mut IndexPageIter<'a>>, highest: Option<&'a [u8]>) -> Self {
        Self {
            iter,
            highest,
            last_index: NULL_INDEX,
        }
    }
}

impl<'a, 'i> ForwardIter for IndexIter<'a, 'i> {
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

impl<'a, 'i> RewindableIter for IndexIter<'a, 'i> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

impl<'a, 'i> SeekableIter<[u8]> for IndexIter<'a, 'i> {
    fn seek(&mut self, target: &[u8]) {
        self.iter.seek(target);
    }
}
