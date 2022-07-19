use std::{
    mem::size_of,
    ops::{Deref, DerefMut},
};

use super::{
    format::{BufReader, BufWriter},
    PageBuf, PageRef,
};

#[repr(u8)]
enum ValueKind {
    Put = 0,
    Delete = 1,
}

impl From<u8> for ValueKind {
    fn from(kind: u8) -> Self {
        match kind {
            0 => Self::Put,
            1 => Self::Delete,
            _ => panic!("invalid data kind"),
        }
    }
}

pub enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

impl<'a> Value<'a> {
    pub fn decode_from(r: &'a mut BufReader) -> Self {
        let kind: ValueKind = r.get_u8().into();
        match kind {
            ValueKind::Put => {
                let value = r.get_length_prefixed_slice();
                Self::Put(value)
            }
            ValueKind::Delete => Self::Delete,
        }
    }

    pub fn encode_to(&self, w: &mut BufWriter) {
        match self {
            Value::Put(value) => {
                w.put_u8(ValueKind::Put as u8);
                w.put_length_prefixed_slice(value);
            }
            Value::Delete => w.put_u8(ValueKind::Delete as u8),
        }
    }

    pub fn encode_size(&self) -> usize {
        1 + match self {
            Value::Put(value) => BufWriter::length_prefixed_slice_size(value),
            Value::Delete => 0,
        }
    }
}

pub struct Record<'a> {
    pub key: &'a [u8],
    pub lsn: u64,
    pub value: Value<'a>,
}

impl<'a> Record<'a> {
    pub fn from_put(key: &'a [u8], lsn: u64, value: &'a [u8]) -> Self {
        Self {
            key,
            lsn,
            value: Value::Put(value),
        }
    }

    pub fn from_delete(key: &'a [u8], lsn: u64) -> Self {
        Self {
            key,
            lsn,
            value: Value::Delete,
        }
    }

    pub fn decode_from(r: &'a mut BufReader) -> Self {
        let key = r.get_length_prefixed_slice();
        let lsn = r.get_u64();
        let value = Value::decode_from(r);
        Self { key, lsn, value }
    }

    pub fn encode_to(&self, w: &mut BufWriter) {
        w.put_length_prefixed_slice(self.key);
        w.put_u64(self.lsn);
        self.value.encode_to(w);
    }

    pub fn encode_size(&self) -> usize {
        BufWriter::length_prefixed_slice_size(self.key)
            + size_of::<u64>()
            + self.value.encode_size()
    }
}

pub struct DataPageLayout {
    size: usize,
}

impl Default for DataPageLayout {
    fn default() -> Self {
        Self { size: 0 }
    }
}

impl DataPageLayout {
    pub fn add(&mut self, record: &Record) {
        self.size += record.encode_size();
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

pub struct DataPageBuf {
    base: PageBuf,
    writer: BufWriter,
}

impl DataPageBuf {
    fn new(mut base: PageBuf) -> Self {
        let writer = BufWriter::new(base.content_mut());
        Self { base, writer }
    }

    pub fn add(&mut self, record: &Record) {
        record.encode_to(&mut self.writer);
    }

    pub fn as_ref(&self) -> DataPageRef<'_> {
        self.base.as_ref().into()
    }
}

impl Deref for DataPageBuf {
    type Target = PageBuf;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl DerefMut for DataPageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl From<PageBuf> for DataPageBuf {
    fn from(base: PageBuf) -> Self {
        Self::new(base)
    }
}

impl From<DataPageBuf> for PageBuf {
    fn from(page: DataPageBuf) -> Self {
        page.base
    }
}

#[derive(Copy, Clone, Debug)]
pub struct DataPageRef<'a>(PageRef<'a>);

impl<'a> DataPageRef<'a> {
    pub fn get(&self, key: &[u8], lsn: u64) -> Option<Value<'a>> {
        todo!()
    }

    pub fn iter(&self) -> DataPageIter<'a> {
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

pub struct DataPageIter<'a>(PageRef<'a>);

impl<'a> Iterator for DataPageIter<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
