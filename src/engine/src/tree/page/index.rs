use super::{PageBuf, PageIter, PageLayout, PagePtr, PageRef};

pub struct IndexValue {
    id: u64,
    ver: u64,
}

pub struct IndexRecord<'a> {
    pub key: &'a [u8],
    pub value: IndexValue,
}

impl<'a> IndexRecord<'a> {}

pub struct IndexPageRef<'a>(&'a [u8]);

impl<'a> IndexPageRef<'a> {
    pub fn get(self, key: &[u8]) -> Option<IndexValue> {
        todo!()
    }

    pub fn iter(self) -> IndexPageIter<'a> {
        todo!()
    }
}

impl<'a> From<PageRef<'a>> for IndexPageRef<'a> {
    fn from(page: PageRef<'a>) -> Self {
        todo!()
    }
}

impl<'a> From<IndexPageRef<'a>> for PageRef<'a> {
    fn from(page: IndexPageRef<'a>) -> Self {
        todo!()
    }
}

pub struct IndexPageIter<'a>(&'a [u8]);

impl<'a> PageIter for IndexPageIter<'a> {
    type Item = IndexRecord<'a>;

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }
}

impl<'a> Iterator for IndexPageIter<'a> {
    type Item = IndexRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct IndexPageBuf(Box<[u8]>);

impl IndexPageBuf {
    pub fn add(&mut self, record: &IndexRecord) {
        todo!()
    }

    pub fn as_ptr(&self) -> PagePtr {
        todo!()
    }
}

impl From<PageBuf> for IndexPageBuf {
    fn from(page: PageBuf) -> Self {
        todo!()
    }
}

impl From<IndexPageBuf> for PageBuf {
    fn from(page: IndexPageBuf) -> Self {
        todo!()
    }
}

pub struct IndexPageLayout {}

impl Default for IndexPageLayout {
    fn default() -> Self {
        Self {}
    }
}

impl IndexPageLayout {
    pub fn add(&mut self, record: &IndexRecord) {
        todo!()
    }
}

impl PageLayout for IndexPageLayout {}
