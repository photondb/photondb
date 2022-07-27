use std::{cmp::Ordering, marker::PhantomData, mem::size_of, ops::Deref};

use super::*;

// TODO: Optimizes the page layout with
// https://cseweb.ucsd.edu//~csjgwang/pubs/ICDE17_BwTree.pdf
#[derive(Default)]
pub struct SortedPageBuilder {
    is_leaf: bool,
    offsets_len: usize,
    payload_size: usize,
}

impl SortedPageBuilder {
    fn add<K, V>(&mut self, key: &K, value: &V)
    where
        K: Encodable,
        V: Encodable,
    {
        self.offsets_len += 1;
        self.payload_size += key.encode_size() + value.encode_size();
    }

    fn size(&self) -> usize {
        PAGE_HEADER_SIZE + self.content_size()
    }

    fn content_size(&self) -> usize {
        self.offsets_len * size_of::<u32>() + self.payload_size
    }

    pub fn is_leaf(self, is_leaf: bool) -> Self {
        Self { is_leaf, ..self }
    }

    pub fn build_default<A>(self, alloc: &A) -> Option<PagePtr>
    where
        A: PageAlloc,
    {
        let ptr = alloc.alloc(self.size());
        ptr.map(|ptr| unsafe {
            SortedPageBuf::new(ptr, self);
            ptr
        })
    }

    pub fn build_from_iter<A, I, K, V>(mut self, iter: &mut I, alloc: &A) -> Option<PagePtr>
    where
        A: PageAlloc,
        I: RewindableIter<Key = K, Value = V>,
        K: Encodable,
        V: Encodable,
    {
        iter.rewind();
        while let Some((key, value)) = iter.next() {
            self.add(key, value);
        }
        let ptr = alloc.alloc(self.size());
        ptr.map(|ptr| unsafe {
            let mut buf = SortedPageBuf::new(ptr, self);
            iter.rewind();
            while let Some((key, value)) = iter.next() {
                buf.add(key, value);
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
    unsafe fn new(mut ptr: PagePtr, builder: SortedPageBuilder) -> Self {
        ptr.set_default();
        ptr.set_leaf(builder.is_leaf);
        let offsets = ptr.content_mut() as *mut u32;
        let payload = offsets.add(builder.offsets_len) as *mut u8;
        Self {
            offsets,
            payload: BufWriter::new(payload),
            current: 0,
        }
    }

    unsafe fn add<K, V>(&mut self, key: &K, value: &V)
    where
        K: Encodable,
        V: Encodable,
    {
        let offset = self.payload.pos() as u32;
        self.offsets.add(self.current).write(offset.to_le());
        self.current += 1;
        key.encode_to(&mut self.payload);
        value.encode_to(&mut self.payload);
    }
}

pub struct SortedPageRef<'a, K, V> {
    base: PageRef<'a>,
    offsets: &'a [u32],
    payload: *const u8,
    _mark: PhantomData<(K, V)>,
}

impl<'a, K, V> SortedPageRef<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    pub unsafe fn new(base: PageRef<'a>) -> Self {
        let offsets_ptr = base.content() as *const u32;
        let offsets_len = (offsets_ptr.read() as usize) / size_of::<u32>();
        let offsets = std::slice::from_raw_parts(offsets_ptr, offsets_len);
        let payload = offsets_ptr.add(offsets_len) as *const u8;
        Self {
            base,
            offsets,
            payload,
            _mark: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    // Returns the first entry that is no less than the target.
    pub fn seek(&self, target: &K) -> Option<(K, V)> {
        self.index(self.rank(target))
    }

    pub fn iter(&self) -> SortedPageIter<'a, K, V> {
        SortedPageIter::new(self.clone())
    }

    pub fn into_iter(self) -> SortedPageIter<'a, K, V> {
        SortedPageIter::new(self)
    }

    fn rank(&self, target: &K) -> usize {
        let mut left = 0;
        let mut right = self.len();
        while left < right {
            let mid = (left + right) / 2;
            let key = unsafe {
                let offset = self.offsets[mid].to_le() as usize;
                let ptr = self.payload.add(offset);
                let mut buf = BufReader::new(ptr);
                K::decode_from(&mut buf)
            };
            match key.cmp(target) {
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
                Ordering::Equal => return mid,
            }
        }
        left
    }

    fn index(&self, index: usize) -> Option<(K, V)> {
        if let Some(&offset) = self.offsets.get(index) {
            unsafe {
                let offset = offset.to_le() as usize;
                let ptr = self.payload.add(offset);
                let mut buf = BufReader::new(ptr);
                let key = K::decode_from(&mut buf);
                let value = V::decode_from(&mut buf);
                Some((key, value))
            }
        } else {
            None
        }
    }
}

impl<'a, K, V> Clone for SortedPageRef<'a, K, V> {
    fn clone(&self) -> Self {
        Self {
            base: self.base,
            offsets: self.offsets,
            payload: self.payload,
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
    current: Option<(K, V)>,
}

impl<'a, K, V> SortedPageIter<'a, K, V>
where
    K: Decodable,
    V: Decodable,
{
    pub fn new(page: SortedPageRef<'a, K, V>) -> Self {
        Self {
            page,
            next: 0,
            current: None,
        }
    }
}

impl<'a, K, V> ForwardIter for SortedPageIter<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    type Key = K;
    type Value = V;

    fn current(&self) -> Option<&(K, V)> {
        self.current.as_ref()
    }

    fn next(&mut self) -> Option<&(K, V)> {
        self.current = self.page.index(self.next).map(|next| {
            self.next += 1;
            next
        });
        self.current.as_ref()
    }
}

impl<'a, K, V> SeekableIter for SortedPageIter<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    fn seek(&mut self, target: &K) {
        self.next = self.page.rank(target);
        self.current = None;
    }
}

impl<'a, K, V> RewindableIter for SortedPageIter<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    fn rewind(&mut self) {
        self.next = 0;
        self.current = None;
    }
}
