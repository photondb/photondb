use std::{borrow::Borrow, cmp::Ordering, marker::PhantomData, mem, ops::Deref, slice};

use super::{
    codec::*, data::*, ItemIter, PageBuf, PageBuilder, PageKind, PageRef, PageTier,
    RewindableIterator, SeekableIterator, SliceIter,
};

/// Builds a sorted page from an iterator.
pub(crate) struct SortedPageBuilder<I> {
    base: PageBuilder,
    iter: Option<I>,
    num_items: usize,
    content_size: usize,
}

impl<I, K, V> SortedPageBuilder<I>
where
    I: RewindableIterator<Item = (K, V)>,
    K: SortedPageKey,
    V: SortedPageValue,
{
    /// Creates a [`SortedPageBuilder`] that will build a page with the given
    /// metadata.
    pub(crate) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self {
            base: PageBuilder::new(tier, kind),
            iter: None,
            num_items: 0,
            content_size: 0,
        }
    }

    /// Creates a [`SortedPageBuilder`] that will build a page from the given
    /// iterator.
    pub(crate) fn with_iter(mut self, mut iter: I) -> Self {
        for (k, v) in &mut iter {
            self.num_items += 1;
            self.content_size += k.encode_size() + v.encode_size();
        }
        self.content_size += self.num_items * mem::size_of::<u32>();
        // We use `u32` to store item offsets, so the content size must not exceed
        // `u32::MAX`.
        assert!(self.content_size <= u32::MAX as usize);
        self.iter = Some(iter);
        self
    }

    /// Returns the size of the page that will be built.
    pub(crate) fn size(&self) -> usize {
        self.base.size(self.content_size)
    }

    /// Builds the page with the given information.
    pub(crate) fn build(mut self, page: &mut PageBuf<'_>) {
        self.base.build(page);
        let content = page.content_mut();
        assert_eq!(content.len(), self.content_size);
        if let Some(mut iter) = self.iter.take() {
            unsafe {
                let mut buf = SortedPageBuf::new(content, self.num_items);
                iter.rewind();
                for (k, v) in iter {
                    buf.add(k, v);
                }
            }
        }
    }
}

impl<K, V> SortedPageBuilder<ItemIter<(K, V)>>
where
    K: SortedPageKey,
    V: SortedPageValue,
{
    pub(crate) fn with_item(self, item: (K, V)) -> Self {
        self.with_iter(ItemIter::new(item))
    }
}

impl<'a, K, V> SortedPageBuilder<SliceIter<'a, (K, V)>>
where
    K: SortedPageKey,
    V: SortedPageValue,
{
    pub(crate) fn with_slice(self, slice: &'a [(K, V)]) -> Self {
        self.with_iter(SliceIter::new(slice))
    }
}

struct SortedPageBuf<K, V> {
    offsets: Encoder,
    payload: Encoder,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> SortedPageBuf<K, V>
where
    K: SortedPageKey,
    V: SortedPageValue,
{
    unsafe fn new(content: &mut [u8], num_items: usize) -> Self {
        let offsets_size = num_items * mem::size_of::<u32>();
        let (offsets, payload) = content.split_at_mut(offsets_size);
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

/// An immutable reference to a sorted page.
#[derive(Clone)]
pub(crate) struct SortedPageRef<'a, K, V> {
    page: PageRef<'a>,
    content: &'a [u8],
    offsets: &'a [u32],
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> SortedPageRef<'a, K, V>
where
    K: SortedPageKey,
    V: SortedPageValue,
{
    pub(crate) fn new(page: PageRef<'a>) -> Self {
        let content = page.content();
        let offsets = unsafe {
            let ptr = content.as_ptr() as *const u32;
            let len = if content.is_empty() {
                0
            } else {
                let size = u32::from_le(ptr.read());
                size as usize / mem::size_of::<u32>()
            };
            slice::from_raw_parts(ptr, len)
        };
        Self {
            page,
            content,
            offsets,
            _marker: PhantomData,
        }
    }

    /// Returns the number of items in the page.
    pub(crate) fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Returns the item at the given index.
    pub(crate) fn get(&self, index: usize) -> Option<(K, V)> {
        if let Some(item) = self.item(index) {
            let mut dec = Decoder::new(item);
            unsafe {
                let k = K::decode_from(&mut dec);
                let v = V::decode_from(&mut dec);
                Some((k, v))
            }
        } else {
            None
        }
    }

    /// Returns the rank of the target in the page.
    ///
    /// If the value is found then [`Result::Ok`] is returned, containing the
    /// index of the matching item. If there are multiple matches, then any
    /// one of the matches could be returned. If the value is not found then
    /// [`Result::Err`] is returned, containing the index where a matching item
    /// could be inserted while maintaining sorted order.
    pub(crate) fn rank<Q: ?Sized>(&self, target: &Q) -> Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        let mut left = 0;
        let mut right = self.len();
        while left < right {
            let mid = (left + right) / 2;
            let key = unsafe {
                let item = self.item(mid).unwrap();
                let mut dec = Decoder::new(item);
                K::decode_from(&mut dec)
            };
            match key.borrow().cmp(target) {
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(left)
    }

    /// Finds a separator to split the page into two halves.
    ///
    /// If a split separator is found then [`Result::Ok`] is returned,
    /// containing the separator and an iterator over the items at and after
    /// the separator.
    pub(crate) fn into_split_iter(self) -> Option<(K, SortedPageIter<'a, K, V>)> {
        if let Some((mid, _)) = self.get(self.len() / 2) {
            let sep = mid.as_split_separator();
            let index = match self.rank(&sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if index > 0 {
                let iter = SortedPageIter::new(self, index);
                return Some((sep, iter));
            }
        }
        None
    }

    fn item(&self, index: usize) -> Option<&[u8]> {
        if let Some(offset) = self.item_offset(index) {
            let next_offset = self.item_offset(index + 1).unwrap_or(self.content.len());
            Some(&self.content[offset..next_offset])
        } else {
            None
        }
    }

    fn item_offset(&self, index: usize) -> Option<usize> {
        self.offsets.get(index).map(|v| u32::from_le(*v) as usize)
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
    K: SortedPageKey,
    V: SortedPageValue,
    T: Into<PageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

/// An iterator over the items in a sorted page.
#[derive(Clone)]
pub(crate) struct SortedPageIter<'a, K, V> {
    page: SortedPageRef<'a, K, V>,
    init: usize,
    next: usize,
}

impl<'a, K, V> SortedPageIter<'a, K, V> {
    /// Creates a [`SortedPageIter`] over items in the given page, starting from
    /// the initial index.
    pub(crate) fn new(page: SortedPageRef<'a, K, V>, init: usize) -> Self {
        Self {
            page,
            init,
            next: init,
        }
    }
}

impl<'a, K, V, T> From<T> for SortedPageIter<'a, K, V>
where
    K: SortedPageKey,
    V: SortedPageValue,
    T: Into<SortedPageRef<'a, K, V>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into(), 0)
    }
}

impl<'a, K, V> Iterator for SortedPageIter<'a, K, V>
where
    K: SortedPageKey,
    V: SortedPageValue,
{
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

impl<'a, K, V> SeekableIterator<K> for SortedPageIter<'a, K, V>
where
    K: SortedPageKey,
    V: SortedPageValue,
{
    fn seek(&mut self, target: &K) {
        self.next = match self.page.rank(target) {
            Ok(i) => i,
            Err(i) => i,
        };
    }
}

impl<'a, K, V> RewindableIterator for SortedPageIter<'a, K, V>
where
    K: SortedPageKey,
    V: SortedPageValue,
{
    fn rewind(&mut self) {
        self.next = self.init;
    }
}

/// Required methods for keys in a sorted page.
pub(crate) trait SortedPageKey: Codec + Clone + Ord {
    /// Returns the raw part of the key.
    fn as_raw(&self) -> &[u8];

    /// Returns a key that can be used as a split separator.
    fn as_split_separator(&self) -> Self;
}

/// Required methods for values in a sorted page.
pub(crate) trait SortedPageValue: Codec + Clone {}

impl<T> SortedPageValue for T where T: Codec + Clone {}

impl Codec for &[u8] {
    fn encode_size(&self) -> usize {
        mem::size_of::<u32>() + self.len()
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u32(self.len() as u32);
        enc.put_slice(self);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let len = dec.get_u32() as usize;
        dec.get_slice(len)
    }
}

impl SortedPageKey for &[u8] {
    fn as_raw(&self) -> &[u8] {
        self
    }

    fn as_split_separator(&self) -> Self {
        self
    }
}

impl Codec for Key<'_> {
    fn encode_size(&self) -> usize {
        self.raw.encode_size() + mem::size_of::<u64>()
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        self.raw.encode_to(enc);
        enc.put_u64(self.lsn);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let raw = Codec::decode_from(dec);
        let lsn = dec.get_u64();
        Self::new(raw, lsn)
    }
}

impl SortedPageKey for Key<'_> {
    fn as_raw(&self) -> &[u8] {
        self.raw
    }

    fn as_split_separator(&self) -> Self {
        // Avoid splitting on the same raw key.
        Key::new(self.raw, u64::MAX)
    }
}

/// These values are persisted to disk, don't change them.
const VALUE_KIND_PUT: u8 = 0;
const VALUE_KIND_DELETE: u8 = 1;

impl Codec for Value<'_> {
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

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let kind = dec.get_u8();
        match kind {
            VALUE_KIND_PUT => Self::Put(dec.get_slice(dec.remaining())),
            VALUE_KIND_DELETE => Self::Delete,
            _ => unreachable!(),
        }
    }
}

impl Codec for Index {
    fn encode_size(&self) -> usize {
        mem::size_of::<u64>() * 2
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u64(self.id);
        enc.put_u64(self.epoch);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let id = dec.get_u64();
        let epoch = dec.get_u64();
        Self::new(id, epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::tests::*;

    #[test]
    fn sorted_page() {
        let data = raw_slice(&[[1], [3], [5]]);
        let owned_page = OwnedSortedPage::from_slice(&data);

        let page = owned_page.as_ref();
        assert_eq!(page.tier(), PageTier::Leaf);
        assert_eq!(page.kind(), PageKind::Data);

        assert_eq!(page.len(), data.len());
        assert_eq!(page.get(0), Some(data[0]));
        assert_eq!(page.get(1), Some(data[1]));
        assert_eq!(page.get(2), Some(data[2]));
        assert_eq!(page.get(3), None);

        assert_eq!(page.rank([0].as_slice()), Err(0));
        assert_eq!(page.rank([1].as_slice()), Ok(0));
        assert_eq!(page.rank([2].as_slice()), Err(1));
        assert_eq!(page.rank([3].as_slice()), Ok(1));
        assert_eq!(page.rank([4].as_slice()), Err(2));
        assert_eq!(page.rank([5].as_slice()), Ok(2));

        let mut iter = SortedPageIter::from(page);
        for _ in 0..2 {
            for (a, b) in (&mut iter).zip(data.clone()) {
                assert_eq!(a, b);
            }
            iter.rewind();
        }
    }

    #[test]
    fn sorted_page_split() {
        // The middle key is ([3], 2), but it should split at ([3], 3).
        let data = key_slice(&[([1], 2), ([1], 1), ([3], 3), ([3], 2), ([3], 1), ([3], 0)]);
        let split_data = key_slice(&[([3], 3), ([3], 2), ([3], 1), ([3], 0)]);
        let owned_page = OwnedSortedPage::from_slice(&data);

        let page = owned_page.as_ref();
        let (split_key, mut split_iter) = page.into_split_iter().unwrap();
        assert_eq!(split_key, Key::new(&[3], u64::MAX));
        for _ in 0..2 {
            for (a, b) in (&mut split_iter).zip(split_data.clone()) {
                assert_eq!(a, b);
            }
            split_iter.rewind();
        }
    }

    #[test]
    fn sorted_page_split_none() {
        {
            let data = raw_slice(&[[1]]);
            let owned_page = OwnedSortedPage::from_slice(&data);
            assert!(owned_page.as_ref().into_split_iter().is_none());
        }
        {
            let data = key_slice(&[([1], 2), ([1], 1), ([3], 3)]);
            let owned_page = OwnedSortedPage::from_slice(&data);
            assert!(owned_page.as_ref().into_split_iter().is_none());
        }
    }
}
