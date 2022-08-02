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
    iter: MergingIter<DataPageIter<'a>>,
    highest: Option<&'a [u8]>,
}

impl<'a> DataNodeIter<'a> {
    pub fn new(iter: MergingIter<DataPageIter<'a>>, highest: Option<&'a [u8]>) -> Self {
        Self { iter, highest }
    }
}

impl<'a> ForwardIter for DataNodeIter<'a> {
    type Key = Key<'a>;
    type Value = Value<'a>;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.iter.last()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
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

impl<'a> SeekableIter for DataNodeIter<'a> {
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<Self::Key>,
    {
        self.iter.seek(target);
    }
}

impl<'a> RewindableIter for DataNodeIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}

pub struct IndexNodeIter<'a> {
    iter: MergingIter<IndexPageIter<'a>>,
    highest: Option<&'a [u8]>,
    last_index: Index,
}

impl<'a> IndexNodeIter<'a> {
    pub fn new(iter: MergingIter<IndexPageIter<'a>>, highest: Option<&'a [u8]>) -> Self {
        Self {
            iter,
            highest,
            last_index: NULL_INDEX,
        }
    }
}

impl<'a> ForwardIter for IndexNodeIter<'a> {
    type Key = &'a [u8];
    type Value = Index;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.iter.last()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        while let Some((key, index)) = self.iter.next().cloned() {
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

impl<'a> SeekableIter for IndexNodeIter<'a> {
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<Self::Key>,
    {
        self.iter.seek(target);
    }
}

impl<'a> RewindableIter for IndexNodeIter<'a> {
    fn rewind(&mut self) {
        self.iter.rewind();
    }
}
