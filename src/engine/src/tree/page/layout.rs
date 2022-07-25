use std::{
    cmp::Ordering,
    marker::PhantomData,
    mem::size_of,
    ops::{Deref, DerefMut},
};

use super::{
    base::PAGE_HEADER_SIZE, codec::*, util::*, PageAlloc, PageBuf, PageIter, PageRef, SingleIter,
};

// TODO: Optimizes the page layout with
// https://cseweb.ucsd.edu//~csjgwang/pubs/ICDE17_BwTree.pdf
#[derive(Default)]
pub struct SortedPageBuilder {
    len: usize,
    size: usize,
}

impl SortedPageBuilder {
    fn len(&self) -> usize {
        self.len
    }

    fn add<K, V>(&mut self, key: &K, value: &V)
    where
        K: Encodable,
        V: Encodable,
    {
        self.len += 1;
        self.size += key.encode_size() + value.encode_size();
    }

    fn size(&self) -> usize {
        size_of::<u64>() * self.len + self.size
    }

    pub fn build(mut self, alloc: &PageAlloc) -> Option<SortedPageBuf> {
        if let Some(buf) = unsafe { alloc.alloc_page(self.size()) } {
            Some(unsafe { SortedPageBuf::new(buf, self) })
        } else {
            None
        }
    }

    pub fn build_from_iter<I, A>(mut self, iter: &mut I, alloc: &PageAlloc) -> Option<SortedPageBuf>
    where
        I: PageIter,
        I::Key: Encodable,
        I::Value: Encodable,
    {
        iter.rewind();
        while let Some((key, value)) = iter.next() {
            self.add(key, value);
        }
        if let Some(buf) = unsafe { alloc.alloc_page(self.size()) } {
            let mut page = unsafe { SortedPageBuf::new(buf, self) };
            iter.rewind();
            while let Some((key, value)) = iter.next() {
                page.add(key, value);
            }
            Some(page)
        } else {
            None
        }
    }
}

pub struct SortedPagePtr {
    base: PagePtr,
    offsets: *mut u32,
    payload: BufWriter,
    current: usize,
}

impl SortedPagePtr {
    unsafe fn new(mut base: PagePtr, builder: SortedPageBuilder) -> Self {
        let offsets = base.content_mut() as *mut u32;
        let payload = offsets.add(builder.len()) as *mut u8;
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

impl Deref for SortedPagePtr {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl DerefMut for SortedPagePtr {
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
            let ptr = unsafe { self.payload.add(self.offsets[mid] as usize) };
            let mut buf = BufReader::new(ptr);
            let key = K::decode_from(&mut buf);
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

impl<'a, K, V> From<PageRef<'a>> for SortedPageRef<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    fn from(page: PageRef<'a>) -> Self {
        unsafe { Self::new(page) }
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

impl<'a, K, V> PageIter for SortedPageIter<'a, K, V>
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
        self.current = self.page.index(self.next);
        if self.current.is_some() {
            self.next += 1;
        }
        self.current.as_ref()
    }

    fn seek(&mut self, target: &K) {
        self.next = self.page.rank(target);
        self.current = None;
    }

    fn rewind(&mut self) {
        self.next = 0;
        self.current = None;
    }
}

pub type DataPageBuf = SortedPageBuf;
pub type DataPageBuilder = SortedPageBuilder;
pub type DataPageRef<'a> = SortedPageRef<'a, Key<'a>, Value<'a>>;
pub type DataPageIter<'a> = SortedPageIter<'a, Key<'a>, Value<'a>>;
pub type IndexPageBuf = SortedPageBuf;
pub type IndexPageBuilder = SortedPageBuilder;
pub type IndexPageRef<'a> = SortedPageRef<'a, &'a [u8], Index>;
pub type IndexPageIter<'a> = SortedPageIter<'a, &'a [u8], Index>;
