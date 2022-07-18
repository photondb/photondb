use std::ops::{Deref, DerefMut};

use super::{PageBuf, PageIter, PageRef};

#[repr(u8)]
pub enum DataKind {
    Put = 0,
    Delete = 1,
}

pub enum DataValue<'a> {
    Put(&'a [u8]),
    Delete,
}

pub struct DataRecord<'a> {
    pub key: &'a [u8],
    pub lsn: u64,
    pub value: DataValue<'a>,
}

impl<'a> DataRecord<'a> {
    pub fn put(key: &'a [u8], lsn: u64, value: &'a [u8]) -> Self {
        Self {
            key,
            lsn,
            value: DataValue::Put(value),
        }
    }

    pub fn delete(key: &'a [u8], lsn: u64) -> Self {
        Self {
            key,
            lsn,
            value: DataValue::Delete,
        }
    }
}

pub struct DataPageLayout {}

impl Default for DataPageLayout {
    fn default() -> Self {
        Self {}
    }
}

impl DataPageLayout {
    pub fn add(&mut self, record: &DataRecord) {
        todo!()
    }

    pub fn size(&self) -> usize {
        todo!()
    }
}

pub struct DataPageBuf(PageBuf);

impl DataPageBuf {
    pub fn add(&mut self, record: &DataRecord) {
        todo!()
    }
}

impl Deref for DataPageBuf {
    type Target = PageBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DataPageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<PageBuf> for DataPageBuf {
    fn from(page: PageBuf) -> Self {
        Self(page)
    }
}

impl From<DataPageBuf> for PageBuf {
    fn from(page: DataPageBuf) -> Self {
        page.0
    }
}

pub struct DataPageRef<'a>(PageRef<'a>);

impl<'a> DataPageRef<'a> {
    pub fn get(self, key: &[u8], lsn: u64) -> Option<DataValue<'a>> {
        todo!()
    }

    pub fn iter(self) -> DataPageIter<'a> {
        todo!()
    }
}

impl<'a> Deref for DataPageRef<'a> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> From<PageRef<'a>> for DataPageRef<'a> {
    fn from(page: PageRef<'a>) -> Self {
        Self(page)
    }
}

impl<'a> From<DataPageRef<'a>> for PageRef<'a> {
    fn from(page: DataPageRef<'a>) -> Self {
        page.0
    }
}

pub struct DataPageIter<'a>(&'a [u8]);

impl<'a> PageIter for DataPageIter<'a> {
    type Item = DataRecord<'a>;

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }
}

impl<'a> Iterator for DataPageIter<'a> {
    type Item = DataRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
