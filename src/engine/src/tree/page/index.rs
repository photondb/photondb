use std::ops::{Deref, DerefMut};

use super::{PageBuf, PageIter, PageRef};

pub struct IndexValue {
    pub id: u64,
    pub ver: u64,
}

pub struct IndexRecord<'a> {
    pub key: &'a [u8],
    pub value: IndexValue,
}

impl<'a> IndexRecord<'a> {}

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

    pub fn size(&self) -> usize {
        todo!()
    }
}

pub struct IndexPageBuf(PageBuf);

impl IndexPageBuf {
    pub fn add(&mut self, record: &IndexRecord) {
        todo!()
    }

    pub fn as_ref(&self) -> IndexPageRef<'_> {
        self.0.as_ref().into()
    }
}

impl Deref for IndexPageBuf {
    type Target = PageBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for IndexPageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<PageBuf> for IndexPageBuf {
    fn from(page: PageBuf) -> Self {
        Self(page)
    }
}

impl From<IndexPageBuf> for PageBuf {
    fn from(page: IndexPageBuf) -> Self {
        page.0
    }
}

#[derive(Copy, Clone, Debug)]
pub struct IndexPageRef<'a>(PageRef<'a>);

impl<'a> IndexPageRef<'a> {
    pub fn get(self, key: &[u8]) -> Option<IndexValue> {
        todo!()
    }

    pub fn iter(self) -> IndexPageIter<'a> {
        todo!()
    }
}

impl<'a> Deref for IndexPageRef<'a> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> From<PageRef<'a>> for IndexPageRef<'a> {
    fn from(page: PageRef<'a>) -> Self {
        Self(page)
    }
}

impl<'a> From<IndexPageRef<'a>> for PageRef<'a> {
    fn from(page: IndexPageRef<'a>) -> Self {
        page.0
    }
}

pub struct IndexPageIter<'a>(PageRef<'a>);

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
