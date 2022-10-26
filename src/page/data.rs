use std::{cmp::Ordering, mem};

use bytes::{Buf, BufMut};

use crate::util::codec::EncodeDecode;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Key<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) lsn: u64,
}

impl<'a> Key<'a> {
    pub(crate) const fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
    }
}

impl Ord for Key<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Order by the raw key ascendingly and the LSN descendingly.
        match self.raw.cmp(other.raw) {
            Ordering::Equal => other.lsn.cmp(&self.lsn),
            o => o,
        }
    }
}

impl PartialOrd for Key<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> EncodeDecode<'a> for &'a [u8] {
    fn size(&self) -> usize {
        4 + self.len()
    }

    fn encode_to(&self, buf: &mut &mut [u8]) {
        buf.put_u32_le(buf.len() as u32);
        buf.put_slice(self)
    }

    fn decode_from(buf: &mut &'a [u8]) -> Self {
        let len = buf.get_u32_le() as usize;
        &buf[0..len]
    }
}

impl<'a> EncodeDecode<'a> for Key<'a> {
    fn size(&self) -> usize {
        self.raw.size() + mem::size_of::<u64>()
    }

    fn encode_to(&self, buf: &mut &mut [u8]) {
        self.raw.encode_to(buf);
        buf.put_u64(self.lsn);
    }

    fn decode_from(buf: &mut &'a [u8]) -> Self {
        let raw = EncodeDecode::decode_from(buf);
        let lsn = buf.get_u64();
        Self::new(raw, lsn)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Range<'a> {
    pub(crate) start: &'a [u8],
    pub(crate) end: Option<&'a [u8]>,
}

impl<'a> Range<'a> {
    pub(crate) const fn full() -> Self {
        Self {
            start: [].as_slice(),
            end: None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

const VALUE_KIND_PUT: u8 = 0;
const VALUE_KIND_DELETE: u8 = 1;

impl<'a> EncodeDecode<'a> for Value<'a> {
    fn size(&self) -> usize {
        1 + match self {
            Self::Put(v) => v.len(),
            Self::Delete => 0,
        }
    }

    fn encode_to(&self, buf: &mut &mut [u8]) {
        match self {
            Value::Put(v) => {
                buf.put_u8(VALUE_KIND_PUT);
                buf.put_slice(v);
            }
            Value::Delete => buf.put_u8(VALUE_KIND_DELETE),
        }
    }

    fn decode_from(buf: &mut &'a [u8]) -> Self {
        let kind = buf.get_u8();
        match kind {
            VALUE_KIND_PUT => Self::Put(buf),
            VALUE_KIND_DELETE => Self::Delete,
            _ => unreachable!(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Index {
    pub(crate) id: u64,
    pub(crate) epoch: u64,
}

impl Index {
    pub(crate) const fn new(id: u64, epoch: u64) -> Self {
        Self { id, epoch }
    }
}

impl EncodeDecode<'_> for Index {
    fn size(&self) -> usize {
        mem::size_of::<u64>() * 2
    }

    fn encode_to(&self, buf: &mut &mut [u8]) {
        buf.put_u64_le(self.id);
        buf.put_u64_le(self.epoch);
    }

    fn decode_from(buf: &mut &[u8]) -> Self {
        let id = buf.get_u64_le();
        let epoch = buf.get_u64_le();
        Self::new(id, epoch)
    }
}
