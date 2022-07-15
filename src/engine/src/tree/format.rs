pub enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

pub struct BaseData<'a>(&'a [u8]);

impl<'a> BaseData<'a> {
    pub fn get(self, key: &[u8]) -> Option<Value<'a>> {
        todo!()
    }
}

impl<'a> From<PageRef<'a>> for BaseData<'a> {
    fn from(page: PageRef<'a>) -> Self {
        todo!()
    }
}

pub struct PageBuf {}

impl PageBuf {}

pub struct DeltaDataBuf {}

impl DeltaDataBuf {
    pub fn add(&mut self, key: &[u8], value: Value<'_>) {
        todo!()
    }
}

pub struct DeltaDataRef<'a>(&'a [u8]);

impl<'a> DeltaDataRef<'a> {
    pub fn get(self, key: &[u8]) -> Option<Value<'a>> {
        todo!()
    }
}

impl<'a> From<PageRef<'a>> for DeltaDataRef<'a> {
    fn from(page: PageRef<'a>) -> Self {
        todo!()
    }
}
