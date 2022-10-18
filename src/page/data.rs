use super::codec::{BufReader, BufWriter, DecodeFrom, EncodeTo};

#[derive(Copy, Clone, Debug)]
pub(crate) struct Key<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) lsn: u64,
}

impl<'a> Key<'a> {
    pub(crate) fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
    }
}

#[derive(Copy, Clone, Debug)]
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

#[derive(Copy, Clone, Debug)]
pub(crate) struct Index {
    pub(crate) id: u64,
    pub(crate) epoch: u64,
}

impl Index {
    pub(crate) fn new(id: u64) -> Self {
        Self { id, epoch: 0 }
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

#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct Range<'a> {
    pub(crate) left: &'a [u8],
    pub(crate) right: Option<&'a [u8]>,
}
