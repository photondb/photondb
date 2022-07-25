use std::{
    cmp::Ordering,
    marker::PhantomData,
    mem::size_of,
    ops::{Deref, DerefMut},
};

use super::{
    Allocator, BufReader, BufWriter, Decodable, Encodable, ForwardIterator, PageAlloc, PagePtr,
    PageRef, PageTags, RandomAccessIterator, SequentialIterator,
};

// TODO: Optimizes the page layout with
// https://cseweb.ucsd.edu//~csjgwang/pubs/ICDE17_BwTree.pdf
#[derive(Default)]
pub struct SortedPageBuilder {
    ver: u64,
    tags: PageTags,
    chain_len: u8,
    chain_next: u64,
    offsets_len: usize,
    payload_size: usize,
}

impl SortedPageBuilder {
    pub fn with_next(next: PageRef<'_>) -> Self {
        Self {
            ver: next.ver(),
            tags: next.tags(),
            chain_len: next.chain_len(),
            chain_next: next.chain_next(),
            offsets_len: 0,
            payload_size: 0,
        }
    }

    pub fn ver(&mut self, ver: u64) {
        self.ver = ver
    }

    pub fn chain_len(&mut self, len: u8) {
        self.chain_len = len
    }

    fn add<K, V>(&mut self, key: &K, value: &V)
    where
        K: Encodable,
        V: Encodable,
    {
        self.offsets_len += 1;
        self.payload_size += key.encode_size() + value.encode_size();
    }

    fn size(&self) -> usize {
        self.offsets_len * size_of::<u32>() + self.payload_size
    }

    pub unsafe fn build<A>(self, alloc: &PageAlloc<A>) -> Option<SortedPage>
    where
        A: Allocator,
    {
        if let Some(ptr) = alloc.alloc_page(self.size()) {
            Some(SortedPage::new(ptr, self))
        } else {
            None
        }
    }

    pub unsafe fn build_from_iter<'a, A, I, K, V>(
        mut self,
        alloc: &PageAlloc<A>,
        into_iter: impl Into<I>,
    ) -> Option<SortedPage>
    where
        A: Allocator,
        I: SequentialIterator<Item = &'a (K, V)>,
        K: Encodable + 'a,
        V: Encodable + 'a,
    {
        let mut iter = into_iter.into();
        while let Some((key, value)) = iter.next() {
            self.add(key, value);
        }
        if let Some(ptr) = alloc.alloc_page(self.size()) {
            let mut buf = SortedPage::new(ptr, self);
            iter.rewind();
            while let Some((key, value)) = iter.next() {
                buf.add(key, value);
            }
            Some(buf)
        } else {
            None
        }
    }
}

pub struct SortedPage {
    base: PagePtr,
    offsets: *mut u32,
    payload: BufWriter,
    current: usize,
}

impl SortedPage {
    unsafe fn new(mut base: PagePtr, builder: SortedPageBuilder) -> Self {
        base.set_ver(builder.ver);
        base.set_tags(builder.tags);
        base.set_chain_len(builder.chain_len);
        base.set_chain_next(builder.chain_next);
        let offsets = base.content_mut() as *mut u32;
        let payload = offsets.add(builder.offsets_len) as *mut u8;
        Self {
            base,
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

impl Deref for SortedPage {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl DerefMut for SortedPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
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
    unsafe fn new(base: PageRef<'a>) -> Self {
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
                let ptr = self.payload.add(self.offsets[mid] as usize);
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
                let ptr = self.payload.add(offset as usize);
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
    K: Decodable + Ord,
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

impl<'a, K, V> ForwardIterator for SortedPageIter<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<&Self::Item> {
        self.current = self.page.index(self.next).map(|next| {
            self.next += 1;
            next
        });
        self.current.as_ref()
    }
}

impl<'a, K, V> SequentialIterator for SortedPageIter<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    fn rewind(&mut self) {
        self.next = 0;
        self.current = None;
    }

    fn current(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}

impl<'a, K, V> RandomAccessIterator for SortedPageIter<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    type Target = K;

    fn seek(&mut self, target: &K) {
        self.next = self.page.rank(target);
        self.current = None;
    }
}
