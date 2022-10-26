use std::{cmp::Ordering, mem};

use crate::util::codec::{BufReader, BufWriter, DecodeFrom, EncodeTo};

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

impl EncodeTo for Key<'_> {
    fn encode_size(&self) -> usize {
        self.raw.encode_size() + mem::size_of_val(&self.lsn)
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        self.raw.encode_to(w);
        w.put_u64(self.lsn);
    }
}

impl DecodeFrom for Key<'_> {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        let raw = r.get_length_prefixed_slice();
        let lsn = r.get_u64();
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

impl EncodeTo for Value<'_> {
    fn encode_size(&self) -> usize {
        match self {
            Self::Put(v) => 1 + v.len(),
            Self::Delete => 1,
        }
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        todo!()
    }
}

impl DecodeFrom for Value<'_> {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        todo!()
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

impl EncodeTo for Index {
    fn encode_size(&self) -> usize {
        todo!()
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        todo!()
    }
}

impl DecodeFrom for Index {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        todo!()
    }
}
