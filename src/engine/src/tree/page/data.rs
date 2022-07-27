use std::{
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    mem::size_of,
    ops::Range,
};

use super::{BufReader, BufWriter};

pub trait Encodable {
    fn encode_size(&self) -> usize;
    unsafe fn encode_to(&self, w: &mut BufWriter);
}

pub trait Decodable {
    unsafe fn decode_from(r: &mut BufReader) -> Self;
}

impl Encodable for &[u8] {
    fn encode_size(&self) -> usize {
        BufWriter::length_prefixed_slice_size(self)
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        w.put_length_prefixed_slice(self);
    }
}

impl Decodable for &[u8] {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        r.get_length_prefixed_slice()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Key<'a> {
    pub raw: &'a [u8],
    pub lsn: u64,
}

impl<'a> Key<'a> {
    pub const fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
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

impl Encodable for Key<'_> {
    fn encode_size(&self) -> usize {
        BufWriter::length_prefixed_slice_size(self.raw) + size_of::<u64>()
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        w.put_length_prefixed_slice(self.raw);
        w.put_u64(self.lsn);
    }
}

impl Decodable for Key<'_> {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        let raw = r.get_length_prefixed_slice();
        let lsn = r.get_u64();
        Self { raw, lsn }
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

#[derive(Copy, Clone, Debug)]
pub enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

impl Encodable for Value<'_> {
    fn encode_size(&self) -> usize {
        1 + match self {
            Value::Put(value) => BufWriter::length_prefixed_slice_size(value),
            Value::Delete => 0,
        }
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        match self {
            Value::Put(value) => {
                w.put_u8(ValueKind::Put as u8);
                w.put_length_prefixed_slice(value);
            }
            Value::Delete => w.put_u8(ValueKind::Delete as u8),
        }
    }
}

impl Decodable for Value<'_> {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        let kind = ValueKind::from(r.get_u8());
        match kind {
            ValueKind::Put => {
                let value = r.get_length_prefixed_slice();
                Self::Put(value)
            }
            ValueKind::Delete => Self::Delete,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Index {
    pub id: u64,
    pub ver: u64,
}

impl Index {
    pub const fn new(id: u64, ver: u64) -> Self {
        Self { id, ver }
    }
}

impl Encodable for Index {
    fn encode_size(&self) -> usize {
        size_of::<u64>() * 2
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        w.put_u64(self.id);
        w.put_u64(self.ver);
    }
}

impl Decodable for Index {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        let id = r.get_u64();
        let ver = r.get_u64();
        Self { id, ver }
    }
}

impl<T: Encodable> Encodable for Range<T> {
    fn encode_size(&self) -> usize {
        self.start.encode_size() + self.end.encode_size()
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        self.start.encode_to(w);
        self.end.encode_to(w);
    }
}

impl<T: Decodable> Decodable for Range<T> {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        let start = T::decode_from(r);
        let end = T::decode_from(r);
        start..end
    }
}
