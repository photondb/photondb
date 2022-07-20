use std::{
    mem::size_of,
    ops::{Deref, DerefMut},
};

use super::{
    format::{BufReader, BufWriter},
    PageBuf, PageIter, PageLayout, PageRef,
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
    fn decode_from(mut r: BufReader) -> Self {
        let kind: ValueKind = r.get_u8().into();
        match kind {
            ValueKind::Put => {
                let value = r.get_length_prefixed_slice();
                Self::Put(value)
            }
            ValueKind::Delete => Self::Delete,
        }
    }

    fn encode_to(&self, w: &mut BufWriter) {
        match self {
            Value::Put(value) => {
                w.put_u8(ValueKind::Put as u8);
                w.put_length_prefixed_slice(value);
            }
            Value::Delete => w.put_u8(ValueKind::Delete as u8),
        }
    }

    fn encode_size(&self) -> usize {
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

    fn decode_from(mut r: BufReader) -> Self {
        let key = r.get_length_prefixed_slice();
        let lsn = r.get_u64();
        let value = Value::decode_from(r);
        Self { key, lsn, value }
    }

    fn encode_to(&self, w: &mut BufWriter) {
        w.put_length_prefixed_slice(self.key);
        w.put_u64(self.lsn);
        self.value.encode_to(w);
    }

    fn encode_size(&self) -> usize {
        BufWriter::length_prefixed_slice_size(self.key)
            + size_of::<u64>()
            + self.value.encode_size()
    }
}

pub struct DataPageLayout {
    len: usize,
    size: usize,
}

impl Default for DataPageLayout {
    fn default() -> Self {
        Self { len: 0, size: 0 }
    }
}

impl DataPageLayout {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn add(&mut self, record: &Record) {
        self.len += 1;
        self.size += record.encode_size();
    }
}

impl PageLayout for DataPageLayout {
    type Buf = DataPageBuf;

    fn size(&self) -> usize {
        size_of::<u32>() * (self.len + 1) + self.size
    }

    fn build(self, base: PageBuf) -> DataPageBuf {
        DataPageBuf::new(base, self)
    }
}

pub struct DataPageBuf {
    base: PageBuf,
    layout: DataPageLayout,
    offsets: *mut u32,
    payload: BufWriter,
    current: usize,
}

// TODO: handle endianness
impl DataPageBuf {
    fn new(mut base: PageBuf, layout: DataPageLayout) -> Self {
        unsafe {
            let ptr = base.content_mut() as *mut u32;
            ptr.write(layout.len() as u32);
            let offsets = ptr.add(1);
            let payload = ptr.add(layout.len() + 1) as *mut u8;
            Self {
                base,
                layout,
                offsets,
                payload: BufWriter::new(payload),
                current: 0,
            }
        }
    }

    pub fn add(&mut self, record: &Record) {
        assert!(self.current < self.layout.len());
        unsafe {
            self.offsets
                .add(self.current)
                .write(self.payload.pos() as u32);
            self.current += 1;
        }
        record.encode_to(&mut self.payload);
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

impl From<DataPageBuf> for PageBuf {
    fn from(buf: DataPageBuf) -> Self {
        buf.base
    }
}

#[derive(Copy, Clone, Debug)]
pub struct DataPageRef<'a>(PageRef<'a>);

impl<'a> DataPageRef<'a> {
    pub fn get(&self, key: &[u8], lsn: u64) -> Option<Value<'a>> {
        todo!()
    }

    pub fn iter(&self) -> DataPageIter<'a> {
        DataPageIter::new(self.0)
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

#[derive(Clone)]
pub struct DataPageIter<'a> {
    offsets: &'a [u32],
    payload: *const u8,
    current: usize,
}

impl<'a> DataPageIter<'a> {
    fn new(base: PageRef<'a>) -> Self {
        unsafe {
            let ptr = base.content() as *const u32;
            let len = ptr.read() as usize;
            let offsets = std::slice::from_raw_parts(ptr.add(1), len);
            let payload = base.content().add(size_of::<u32>() * (len + 1));
            Self {
                offsets,
                payload,
                current: 0,
            }
        }
    }
}

impl<'a> PageIter for DataPageIter<'a> {
    type Item = Record<'a>;

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn get(&self, n: usize) -> Option<Self::Item> {
        if let Some(&offset) = self.offsets.get(n) {
            unsafe {
                let ptr = self.payload.add(offset as usize);
                Some(Record::decode_from(BufReader::new(ptr)))
            }
        } else {
            None
        }
    }

    fn next(&mut self) -> Option<Self::Item> {
        self.get(self.current).map(|record| {
            self.current += 1;
            record
        })
    }
}
