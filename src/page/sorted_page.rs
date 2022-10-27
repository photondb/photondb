use std::{marker::PhantomData, mem, ops::Deref};

use super::{
    codec::*, data::*, PageBuf, PageBuilder, PageKind, PageRef, PageTier, RewindableIterator,
    SeekableIterator,
};

pub(crate) struct SortedPageBuilder<I> {
    base: PageBuilder,
    iter: Option<I>,
    size: usize,
    num_items: usize,
}

impl<'a, I, K, V> SortedPageBuilder<I>
where
    I: RewindableIterator<Item = (K, V)>,
    K: EncodeTo + DecodeFrom,
    V: EncodeTo + DecodeFrom,
{
    pub(crate) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self {
            base: PageBuilder::new(tier, kind),
            iter: None,
            size: 0,
            num_items: 0,
        }
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn with_iter(mut self, mut iter: I) -> Self {
        for (k, v) in &mut iter {
            self.size += k.encode_size() + v.encode_size();
            self.num_items += 1;
        }
        self.size += self.num_items * mem::size_of::<u32>();
        self.iter = Some(iter);
        self
    }

    pub(crate) fn build(mut self, page: &'a mut PageBuf<'_>) {
        assert_eq!(page.size(), self.size);
        self.base.build(page);
        if let Some(mut iter) = self.iter.take() {
            unsafe {
                let mut buf = SortedPageBuf::new(page, self.num_items);
                iter.rewind();
                for (k, v) in iter {
                    buf.add(k, v);
                }
            }
        }
    }
}

struct SortedPageBuf<K, V> {
    offsets: Encoder,
    payload: Encoder,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> SortedPageBuf<K, V>
where
    K: EncodeTo + DecodeFrom,
    V: EncodeTo + DecodeFrom,
{
    unsafe fn new(page: &mut PageBuf<'_>, num_items: usize) -> Self {
        let content = page.content_mut();
        let offsets_len = num_items * mem::size_of::<u32>();
        let (offsets, payload) = content.split_at_mut(offsets_len);
        Self {
            offsets: Encoder::new(offsets),
            payload: Encoder::new(payload),
            _marker: PhantomData,
        }
    }

    unsafe fn add(&mut self, key: K, value: V) {
        let offset = self.offsets.len() + self.payload.offset();
        self.offsets.put_u32(offset as u32);
        key.encode_to(&mut self.payload);
        value.encode_to(&mut self.payload);
    }
}

pub(crate) struct SortedPageRef<'a, K, V> {
    page: PageRef<'a>,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> SortedPageRef<'a, K, V> {
    pub(crate) fn new(page: PageRef<'a>) -> Self {
        Self {
            page,
            _marker: PhantomData,
        }
    }

    pub(crate) fn len(&self) -> usize {
        todo!()
    }

    pub(crate) fn get(&self, index: usize) -> Option<(K, V)> {
        todo!()
    }

    pub(crate) fn rank(&self, target: &K) -> Result<usize, usize> {
        todo!()
    }

    pub(crate) fn split(&self) -> Option<(K, SortedPageIter<'a, K, V>)> {
        todo!()
    }
}

impl<'a, K, V> Deref for SortedPageRef<'a, K, V> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl<'a, K, V, T> From<T> for SortedPageRef<'a, K, V>
where
    T: Into<PageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

pub(crate) struct SortedPageIter<'a, K, V> {
    page: SortedPageRef<'a, K, V>,
    next: usize,
}

impl<'a, K, V> SortedPageIter<'a, K, V> {
    pub(crate) fn new(page: SortedPageRef<'a, K, V>) -> Self {
        Self { page, next: 0 }
    }
}

impl<'a, K, V, T> From<T> for SortedPageIter<'a, K, V>
where
    T: Into<SortedPageRef<'a, K, V>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

impl<'a, K, V> Iterator for SortedPageIter<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.page.get(self.next) {
            self.next += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<'a, K, V, T> SeekableIterator<T> for SortedPageIter<'a, K, V> {
    fn seek(&mut self, target: &T) {
        todo!()
    }
}

impl<'a, K, V> RewindableIterator for SortedPageIter<'a, K, V> {
    fn rewind(&mut self) {
        self.next = 0;
    }
}

impl EncodeTo for &[u8] {
    fn encode_size(&self) -> usize {
        mem::size_of::<u32>() + self.len()
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u32(self.len() as u32);
        enc.put_slice(self);
    }
}

impl DecodeFrom for &[u8] {
    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let len = dec.get_u32() as usize;
        dec.get_slice(len)
    }
}

impl EncodeTo for Key<'_> {
    fn encode_size(&self) -> usize {
        self.raw.encode_size() + mem::size_of::<u64>()
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        self.raw.encode_to(enc);
        enc.put_u64(self.lsn);
    }
}

impl DecodeFrom for Key<'_> {
    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let raw = DecodeFrom::decode_from(dec);
        let lsn = dec.get_u64();
        Self::new(raw, lsn)
    }
}

const VALUE_KIND_PUT: u8 = 0;
const VALUE_KIND_DELETE: u8 = 1;

impl EncodeTo for Value<'_> {
    fn encode_size(&self) -> usize {
        1 + match self {
            Self::Put(v) => v.len(),
            Self::Delete => 0,
        }
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        match self {
            Value::Put(v) => {
                enc.put_u8(VALUE_KIND_PUT);
                enc.put_slice(v);
            }
            Value::Delete => enc.put_u8(VALUE_KIND_DELETE),
        }
    }
}

impl DecodeFrom for Value<'_> {
    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let kind = dec.get_u8();
        match kind {
            VALUE_KIND_PUT => Self::Put(dec.get_slice(dec.remaining())),
            VALUE_KIND_DELETE => Self::Delete,
            _ => unreachable!(),
        }
    }
}

impl EncodeTo for Index {
    fn encode_size(&self) -> usize {
        mem::size_of::<u64>() * 2
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u64(self.id);
        enc.put_u64(self.epoch);
    }
}

impl DecodeFrom for Index {
    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let id = dec.get_u64();
        let epoch = dec.get_u64();
        Self::new(id, epoch)
    }
}
