use std::{
    cmp::Ordering,
    marker::PhantomData,
    mem::size_of,
    ops::{Deref, DerefMut},
};

use super::{PageBuf, PageIter, PageRef, PAGE_HEADER_SIZE};

pub trait Encodable {
    fn encode_to(&self, w: &mut BufWriter);
    fn encode_size(&self) -> usize;
}

pub trait Decodable {
    fn decode_from(r: &mut BufReader) -> Self;
}

pub struct BufReader {
    ptr: *const u8,
    pos: usize,
}

macro_rules! impl_get {
    ($name:ident, $t:ty) => {
        pub fn $name(&mut self) -> $t {
            unsafe {
                let ptr = self.ptr.add(self.pos) as *const $t;
                self.pos += size_of::<$t>();
                ptr.read()
            }
        }
    };
}

impl BufReader {
    pub fn new(ptr: *const u8) -> Self {
        Self { ptr, pos: 0 }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    impl_get!(get_u8, u8);
    impl_get!(get_u16, u16);
    impl_get!(get_u32, u32);
    impl_get!(get_u64, u64);

    pub fn get_slice<'a>(&mut self, len: usize) -> &'a [u8] {
        unsafe {
            let ptr = self.ptr.add(self.pos);
            self.pos += len;
            std::slice::from_raw_parts(ptr, len)
        }
    }

    pub fn get_length_prefixed_slice<'a>(&mut self) -> &'a [u8] {
        let len = self.get_u32();
        self.get_slice(len as usize)
    }
}

pub struct BufWriter {
    ptr: *mut u8,
    pos: usize,
}

macro_rules! impl_put {
    ($name:ident, $t:ty) => {
        pub fn $name(&mut self, v: $t) {
            unsafe {
                let ptr = self.ptr.add(self.pos) as *mut $t;
                ptr.write(v);
                self.pos += size_of::<$t>();
            }
        }
    };
}

impl BufWriter {
    pub fn new(ptr: *mut u8) -> Self {
        Self { ptr, pos: 0 }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    impl_put!(put_u8, u8);
    impl_put!(put_u16, u16);
    impl_put!(put_u32, u32);
    impl_put!(put_u64, u64);

    pub fn put_slice(&mut self, slice: &[u8]) {
        unsafe {
            let ptr = self.ptr.add(self.pos) as *mut u8;
            ptr.copy_from(slice.as_ptr(), slice.len());
            self.pos += slice.len();
        }
    }

    pub fn put_length_prefixed_slice(&mut self, slice: &[u8]) {
        self.put_u32(slice.len() as u32);
        self.put_slice(slice);
    }

    pub fn length_prefixed_slice_size(slice: &[u8]) -> usize {
        size_of::<u32>() + slice.len()
    }
}

// TODO: Optimizes the page layout with
// https://cseweb.ucsd.edu//~csjgwang/pubs/ICDE17_BwTree.pdf
#[derive(Default)]
pub struct SortedPageLayout {
    len: usize,
    size: usize,
}

impl SortedPageLayout {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn add<K, V>(&mut self, key: &K, value: &V)
    where
        K: Encodable,
        V: Encodable,
    {
        self.len += 1;
        self.size += key.encode_size() + value.encode_size();
    }

    pub fn size(&self) -> usize {
        PAGE_HEADER_SIZE + (self.len + 1) * size_of::<u64>() + self.size
    }

    pub fn into_buf(self, base: PageBuf) -> SortedPageBuf {
        assert_eq!(base.size(), self.size());
        unsafe { SortedPageBuf::new(base, self) }
    }
}

pub struct SortedPageBuf {
    base: PageBuf,
    offsets: *mut u32,
    payload: BufWriter,
    current: usize,
}

// TODO: handle endianness
impl SortedPageBuf {
    unsafe fn new(mut base: PageBuf, layout: SortedPageLayout) -> Self {
        let ptr = base.content_mut() as *mut u32;
        ptr.write(layout.len() as u32);
        let offsets = ptr.add(1);
        let payload = ptr.add(layout.len() + 1) as *mut u8;
        Self {
            base,
            offsets,
            payload: BufWriter::new(payload),
            current: 0,
        }
    }

    pub fn add<K, V>(&mut self, key: &K, value: &V)
    where
        K: Encodable,
        V: Encodable,
    {
        unsafe {
            self.offsets
                .add(self.current)
                .write(self.payload.pos() as u32);
            self.current += 1;
        }
        key.encode_to(&mut self.payload);
        value.encode_to(&mut self.payload);
    }
}

impl Deref for SortedPageBuf {
    type Target = PageBuf;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl DerefMut for SortedPageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl From<SortedPageBuf> for PageBuf {
    fn from(page: SortedPageBuf) -> Self {
        page.base
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
        let ptr = base.content() as *const u32;
        let len = ptr.read() as usize;
        let offsets = std::slice::from_raw_parts(ptr.add(1), len);
        let payload = base.content().add(size_of::<u32>() * (len + 1));
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

    // Returns the first entry at or past the target.
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
    index: usize,
    entry: Option<(K, V)>,
}

impl<'a, K, V> SortedPageIter<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    pub fn new(page: SortedPageRef<'a, K, V>) -> Self {
        Self {
            page,
            index: 0,
            entry: None,
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

    fn peek(&self) -> Option<&(K, V)> {
        self.entry.as_ref()
    }

    fn next(&mut self) -> Option<&(K, V)> {
        self.entry = self.page.index(self.index).map(|entry| {
            self.index += 1;
            entry
        });
        self.entry.as_ref()
    }

    fn seek(&mut self, target: &K) {
        self.index = self.page.rank(target);
        self.entry = None;
    }

    fn rewind(&mut self) {
        self.index = 0;
        self.entry = None;
    }
}
