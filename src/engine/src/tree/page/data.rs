use super::{PageBuf, PageIter, PageLayout, PagePtr, PageRef};

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

pub struct DataPageRef<'a>(&'a [u8]);

impl<'a> DataPageRef<'a> {
    pub fn get(self, key: &[u8]) -> Option<DataValue<'a>> {
        todo!()
    }

    pub fn iter(self) -> DataPageIter<'a> {
        todo!()
    }
}

impl<'a> From<PageRef<'a>> for DataPageRef<'a> {
    fn from(page: PageRef<'a>) -> Self {
        todo!()
    }
}

impl<'a> From<DataPageRef<'a>> for PageRef<'a> {
    fn from(page: DataPageRef<'a>) -> Self {
        todo!()
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

pub struct DataPageBuf(Box<[u8]>);

impl DataPageBuf {
    pub fn add(&mut self, record: &DataRecord) {
        todo!()
    }

    pub fn as_ptr(&self) -> PagePtr {
        todo!()
    }
}

impl From<PageBuf> for DataPageBuf {
    fn from(page: PageBuf) -> Self {
        todo!()
    }
}

impl From<DataPageBuf> for PageBuf {
    fn from(page: DataPageBuf) -> Self {
        todo!()
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

impl PageLayout for DataPageLayout {}
