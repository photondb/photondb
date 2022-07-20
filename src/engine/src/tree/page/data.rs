use std::{
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    mem::size_of,
    ops::{Deref, DerefMut},
};

use super::{
    format::{BufReader, BufWriter},
    PageBuf, PageIter, PageLayout, PageRef,
};

#[derive(Copy, Clone, Debug)]
pub struct Key<'a> {
    pub raw: &'a [u8],
    pub lsn: u64,
}

impl<'a> Key<'a> {
    pub fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
    }

    fn decode_from(r: &mut BufReader) -> Self {
        let raw = r.get_length_prefixed_slice();
        let lsn = r.get_u64();
        Self { raw, lsn }
    }

    fn encode_to(&self, w: &mut BufWriter) {
        w.put_length_prefixed_slice(self.raw);
        w.put_u64(self.lsn);
    }

    fn encode_size(&self) -> usize {
        BufWriter::length_prefixed_slice_size(self.raw) + size_of::<u64>()
    }
}

impl Eq for Key<'_> {}

impl PartialEq for Key<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw && self.lsn == other.lsn
    }
}

impl Ord for Key<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.raw.cmp(other.raw) {
            Ordering::Equal => self.lsn.cmp(&other.lsn).reverse(),
            o => o,
        }
    }
}

impl PartialOrd for Key<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Copy, Clone, Debug)]
pub enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

impl Value<'_> {
    fn decode_from(r: &mut BufReader) -> Self {
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

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
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

    pub fn add(&mut self, key: Key<'_>, value: Value<'_>) {
        self.len += 1;
        self.size += key.encode_size() + value.encode_size();
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

    pub fn add(&mut self, key: Key<'_>, value: Value<'_>) {
        assert!(self.current < self.layout.len());
        unsafe {
            self.offsets
                .add(self.current)
                .write(self.payload.pos() as u32);
            self.current += 1;
        }
        key.encode_to(&mut self.payload);
        value.encode_to(&mut self.payload);
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
    pub fn get(&self, target: Key<'a>) -> Option<Value<'a>> {
        let mut iter = self.iter();
        if let Some((key, value)) = iter.seek(target) {
            if key == target {
                return Some(value);
            }
        }
        return None;
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
    current_index: usize,
    current_entry: Option<(Key<'a>, Value<'a>)>,
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
                current_index: 0,
                current_entry: None,
            }
        }
    }

    fn get(&self, index: usize) -> Option<(Key<'a>, Value<'a>)> {
        if let Some(&offset) = self.offsets.get(index) {
            unsafe {
                let ptr = self.payload.add(offset as usize);
                let mut buf = BufReader::new(ptr);
                let key = Key::decode_from(&mut buf);
                let value = Value::decode_from(&mut buf);
                Some((key, value))
            }
        } else {
            None
        }
    }
}

impl<'a> PageIter for DataPageIter<'a> {
    type Key = Key<'a>;
    type Value = Value<'a>;

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn peek(&self) -> Option<(Self::Key, Self::Value)> {
        self.current_entry
    }

    fn next(&mut self) -> Option<(Self::Key, Self::Value)> {
        self.current_entry = self.get(self.current_index);
        if self.current_entry.is_some() {
            self.current_index += 1;
        }
        self.current_entry
    }

    fn seek(&mut self, target: Self::Key) -> Option<(Self::Key, Self::Value)> {
        let mut left = 0;
        let mut right = self.len();
        while left < right {
            let mid = (left + right) / 2;
            let ptr = unsafe { self.payload.add(self.offsets[mid] as usize) };
            let mut buf = BufReader::new(ptr);
            let key = Key::decode_from(&mut buf);
            match key.cmp(&target) {
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
                Ordering::Equal => {
                    let value = Value::decode_from(&mut buf);
                    return Some((key, value));
                }
            }
        }
        None
    }
}
