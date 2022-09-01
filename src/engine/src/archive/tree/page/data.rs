use std::{
    borrow::Borrow,
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    mem::size_of,
};

use crate::util::{BufReader, BufWriter};

pub trait Compare<T: ?Sized> {
    fn compare(&self, other: &T) -> Ordering;
}

/// An interface to encode data to a buffer.
pub trait EncodeTo {
    /// Returns the exact size to encode this object.
    fn encode_size(&self) -> usize;

    /// Encodes this object to a `BufWriter`.
    ///
    /// # Safety
    ///
    /// The `BufWriter` must be initialized with enough space to encode this object.
    unsafe fn encode_to(&self, w: &mut BufWriter);
}

/// An interface to decode data from a buffer.
pub trait DecodeFrom {
    /// Decodes an object from a `BufReader`.
    ///
    /// # Safety
    ///
    /// The `BufReader` must be initialized with enough data to decode such an object.
    unsafe fn decode_from(r: &mut BufReader) -> Self;
}

impl Compare<u64> for u64 {
    fn compare(&self, other: &u64) -> Ordering {
        self.cmp(other)
    }
}

impl EncodeTo for u64 {
    fn encode_size(&self) -> usize {
        size_of::<u64>()
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        w.put_u64(*self);
    }
}

impl DecodeFrom for u64 {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        r.get_u64()
    }
}

impl<T: Borrow<[u8]> + ?Sized> Compare<&[u8]> for T {
    fn compare(&self, other: &&[u8]) -> Ordering {
        self.borrow().cmp(other)
    }
}

impl EncodeTo for &[u8] {
    fn encode_size(&self) -> usize {
        BufWriter::length_prefixed_slice_size(self)
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        w.put_length_prefixed_slice(self);
    }
}

impl DecodeFrom for &[u8] {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        r.get_length_prefixed_slice()
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Key<'a> {
    pub raw: &'a [u8],
    pub lsn: u64,
}

impl<'a> Key<'a> {
    pub const fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
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

impl EncodeTo for Key<'_> {
    fn encode_size(&self) -> usize {
        BufWriter::length_prefixed_slice_size(self.raw) + size_of::<u64>()
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        w.put_length_prefixed_slice(self.raw);
        w.put_u64(self.lsn);
    }
}

impl DecodeFrom for Key<'_> {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        let raw = r.get_length_prefixed_slice();
        let lsn = r.get_u64();
        Self { raw, lsn }
    }
}

impl Compare<Key<'_>> for Key<'_> {
    fn compare(&self, other: &Key<'_>) -> Ordering {
        self.cmp(other)
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

impl EncodeTo for Value<'_> {
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

impl DecodeFrom for Value<'_> {
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

impl<'a> From<Value<'a>> for Option<&'a [u8]> {
    fn from(v: Value<'a>) -> Self {
        match v {
            Value::Put(value) => Some(value),
            Value::Delete => None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Index {
    pub id: u64,
    pub ver: u64,
}

impl Index {
    pub const fn new(id: u64, ver: u64) -> Self {
        Self { id, ver }
    }
}

impl EncodeTo for Index {
    fn encode_size(&self) -> usize {
        size_of::<u64>() * 2
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        w.put_u64(self.id);
        w.put_u64(self.ver);
    }
}

impl DecodeFrom for Index {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        let id = r.get_u64();
        let ver = r.get_u64();
        Self::new(id, ver)
    }
}

pub type DataItem<'a> = (Key<'a>, Value<'a>);
pub type IndexItem<'a> = (&'a [u8], Index);

impl<K, V> Compare<(K, V)> for (K, V)
where
    K: Ord,
{
    fn compare(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}
