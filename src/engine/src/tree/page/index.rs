use std::mem::size_of;

use super::format::*;

impl Encodable for &[u8] {
    fn encode_to(&self, w: &mut BufWriter) {
        w.put_length_prefixed_slice(self);
    }
    fn encode_size(&self) -> usize {
        BufWriter::length_prefixed_slice_size(self)
    }
}

impl Decodable for &[u8] {
    fn decode_from(r: &mut BufReader) -> Self {
        r.get_length_prefixed_slice()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Index {
    pub id: u64,
    pub ver: u64,
}

impl Encodable for Index {
    fn encode_to(&self, w: &mut BufWriter) {
        w.put_u64(self.id);
        w.put_u64(self.ver);
    }

    fn encode_size(&self) -> usize {
        size_of::<u64>() * 2
    }
}

impl Decodable for Index {
    fn decode_from(r: &mut BufReader) -> Self {
        let id = r.get_u64();
        let ver = r.get_u64();
        Self { id, ver }
    }
}

pub type IndexPageLayout = SortedPageLayout;
pub type IndexPageBuf = SortedPageBuf;
pub type IndexPageRef<'a> = SortedPageRef<'a, &'a [u8], Index>;
pub type IndexPageIter<'a> = SortedPageIter<'a, &'a [u8], Index>;
