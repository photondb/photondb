use std::{cmp::Ordering, marker::PhantomData, mem::size_of, ops::Deref};

use super::*;
use crate::util::{BufReader, BufWriter};

/// A builder to create pages with sorted entries.
pub struct SortedPageBuilder {
    base: PageBuilder,
    offsets_size: usize,
    payload_size: usize,
}

// TODO: Optimizes the page layout with
// https://cseweb.ucsd.edu//~csjgwang/pubs/ICDE17_BwTree.pdf
impl SortedPageBuilder {
    pub fn new(kind: PageKind, is_data: bool) -> Self {
        Self {
            base: PageBuilder::new(kind, is_data),
            offsets_size: 0,
            payload_size: 0,
        }
    }

    fn add<K, V>(&mut self, key: &K, value: &V)
    where
        K: EncodeTo,
        V: EncodeTo,
    {
        self.offsets_size += size_of::<u32>();
        self.payload_size += key.encode_size() + value.encode_size();
    }

    fn size(&self) -> usize {
        self.offsets_size + self.payload_size
    }

    /// Builds an empty page.
    pub fn build<A>(self, alloc: &A) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        let ptr = self.base.build(alloc, self.size());
        ptr.map(|ptr| unsafe {
            SortedPageBuf::new(ptr, self);
            ptr
        })
    }

    /// Builds a page with entries from the given iterator.
    pub fn build_from_iter<A, I, K, V>(
        mut self,
        alloc: &A,
        iter: &mut I,
    ) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
        I: ForwardIter<Item = (K, V)>,
        K: EncodeTo,
        V: EncodeTo,
    {
        iter.rewind();
        while let Some((k, v)) = iter.next() {
            self.add(k, v);
        }
        let ptr = self.base.build(alloc, self.size());
        ptr.map(|ptr| unsafe {
            let mut buf = SortedPageBuf::new(ptr, self);
            iter.rewind();
            while let Some((k, v)) = iter.next() {
                buf.add(k, v);
            }
            ptr
        })
    }
}

struct SortedPageBuf {
    offsets: *mut u32,
    payload: BufWriter,
    current: usize,
}

impl SortedPageBuf {
    unsafe fn new(mut base: PagePtr, builder: SortedPageBuilder) -> Self {
        let offsets = base.content_mut() as *mut u32;
        let mut payload = BufWriter::new(base.content_mut());
        payload.skip(builder.offsets_size);
        Self {
            offsets,
            payload,
            current: 0,
        }
    }

    unsafe fn add<K, V>(&mut self, key: &K, value: &V)
    where
        K: EncodeTo,
        V: EncodeTo,
    {
        let offset = self.payload.offset_from(self.offsets as *mut u8) as u32;
        self.offsets.add(self.current).write(offset.to_le());
        self.current += 1;
        key.encode_to(&mut self.payload);
        value.encode_to(&mut self.payload);
    }
}

/// An immutable reference to a sorted page.
pub struct SortedPageRef<'a, K, V> {
    base: PageRef<'a>,
    offsets: &'a [u32],
    _mark: PhantomData<(K, V)>,
}

impl<'a, K, V> SortedPageRef<'a, K, V>
where
    K: DecodeFrom + Ord,
    V: DecodeFrom,
{
    pub unsafe fn new(base: PageRef<'a>) -> Self {
        let offsets_ptr = base.content() as *const u32;
        let offsets_len = if base.content_size() == 0 {
            0
        } else {
            offsets_ptr.read() as usize / size_of::<u32>()
        };
        let offsets = std::slice::from_raw_parts(offsets_ptr, offsets_len);
        Self {
            base,
            offsets,
            _mark: PhantomData,
        }
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Returns the entry at the given position.
    pub fn get(&self, index: usize) -> Option<(K, V)> {
        if let Some(&offset) = self.offsets.get(index) {
            unsafe {
                let ptr = self.content_at(offset);
                let mut buf = BufReader::new(ptr);
                let key = K::decode_from(&mut buf);
                let value = V::decode_from(&mut buf);
                Some((key, value))
            }
        } else {
            None
        }
    }

    /// Searches `target` in the page.
    ///
    /// If `target` is found, returns `Result::Ok` with its index.
    /// If `target is not found, returns `Result::Err` with the index where `target` could be
    /// inserted while maintaining the sorted order.
    pub fn search<T>(&self, target: &T) -> Result<usize, usize>
    where
        T: Compare<K> + ?Sized,
    {
        let mut left = 0;
        let mut right = self.len();
        while left < right {
            let mid = (left + right) / 2;
            let key = unsafe {
                let ptr = self.content_at(self.offsets[mid]);
                let mut buf = BufReader::new(ptr);
                K::decode_from(&mut buf)
            };
            match target.compare(&key) {
                Ordering::Less => right = mid,
                Ordering::Greater => left = mid + 1,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(left)
    }

    /// Returns the first entry that is no less than `target`.
    pub fn seek<T>(&self, target: &T) -> Option<(K, V)>
    where
        T: Compare<K> + ?Sized,
    {
        let rank = match self.search(target) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.get(rank)
    }

    /// Returns the first entry that is no greater than `target`.
    pub fn seek_back<T>(&self, target: &T) -> Option<(K, V)>
    where
        T: Compare<K> + ?Sized,
    {
        match self.search(target) {
            Ok(i) => self.get(i),
            Err(i) => i.checked_sub(1).and_then(|i| self.get(i)),
        }
    }

    fn content_at(&self, offset: u32) -> *const u8 {
        let offset = offset.to_le() as usize;
        unsafe { self.base.content().add(offset) }
    }
}

impl<'a, K, V> Clone for SortedPageRef<'a, K, V> {
    fn clone(&self) -> Self {
        Self {
            base: self.base,
            offsets: self.offsets,
            _mark: PhantomData,
        }
    }
}

impl<'a, K, V> Deref for SortedPageRef<'a, K, V> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'a, K, V> From<SortedPageRef<'a, K, V>> for PageRef<'a> {
    fn from(page: SortedPageRef<'a, K, V>) -> Self {
        page.base
    }
}

pub struct SortedPageIter<'a, K, V> {
    page: SortedPageRef<'a, K, V>,
    next: usize,
    last: Option<(K, V)>,
}

impl<'a, K, V> SortedPageIter<'a, K, V>
where
    K: DecodeFrom + Ord,
    V: DecodeFrom,
{
    pub fn new(page: SortedPageRef<'a, K, V>) -> Self {
        Self {
            page,
            next: 0,
            last: None,
        }
    }
}

impl<'a, K, V> ForwardIter for SortedPageIter<'a, K, V>
where
    K: DecodeFrom + Ord,
    V: DecodeFrom,
{
    type Item = (K, V);

    fn last(&self) -> Option<&Self::Item> {
        self.last.as_ref()
    }

    fn next(&mut self) -> Option<&Self::Item> {
        self.last = self.page.get(self.next).map(|next| {
            self.next += 1;
            next
        });
        self.last.as_ref()
    }

    fn rewind(&mut self) {
        self.next = 0;
        self.last = None;
    }

    fn skip(&mut self, n: usize) {
        self.next = self.next.saturating_add(n).min(self.page.len());
    }

    fn skip_all(&mut self) {
        self.next = self.page.len();
    }
}

impl<'a, K, V, T> SeekableIter<T> for SortedPageIter<'a, K, V>
where
    K: DecodeFrom + Ord,
    V: DecodeFrom,
    T: Compare<K> + ?Sized,
{
    fn seek(&mut self, target: &T) {
        self.next = match self.page.search(target) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.last = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::Sysalloc;

    #[test]
    fn data_page() {
        let data = [(1, 0), (2, 0), (4, 0), (7, 0), (8, 0)];
        let mut iter = SliceIter::from(&data);
        let page = SortedPageBuilder::new(PageKind::Data, true)
            .build_from_iter(&Sysalloc, &mut iter)
            .unwrap();
        assert_eq!(page.kind(), PageKind::Data);
        assert_eq!(page.is_data(), true);

        let page = unsafe { SortedPageRef::new(page.into()) };
        assert_eq!(page.len(), data.len());
        assert_eq!(page.seek(&0), Some((1, 0)));
        assert_eq!(page.seek_back(&0), None);
        assert_eq!(page.seek(&3), Some((4, 0)));
        assert_eq!(page.seek_back(&3), Some((2, 0)));
        assert_eq!(page.seek(&9), None);
        assert_eq!(page.seek_back(&9), Some((8, 0)));

        let mut iter = SortedPageIter::new(page);
        assert_eq!(iter.last(), None);
        for _ in 0..2 {
            for item in data.iter() {
                assert_eq!(iter.next(), Some(item));
                assert_eq!(iter.last(), Some(item));
            }
            iter.rewind();
        }
    }
}
