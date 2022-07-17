use std::{
    alloc::Layout,
    ops::{Deref, DerefMut},
};

use super::{PageBuf, PageIter, PageLayout, PageRef};

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
    pub lsn: u64,
    pub key: &'a [u8],
    pub value: DataValue<'a>,
}

impl<'a> DataRecord<'a> {
    pub fn put(lsn: u64, key: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            lsn,
            key,
            value: DataValue::Put(value),
        }
    }

    pub fn delete(lsn: u64, key: &'a [u8]) -> Self {
        Self {
            lsn,
            key,
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
}

impl PageLayout for DataPageLayout {
    fn layout(&self) -> Layout {
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
    pub fn get(self, key: &[u8]) -> Option<DataValue<'a>> {
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
