use std::{cmp::Ordering, marker::PhantomData, mem::size_of, ops::Deref, slice};

use super::*;

/// A builder to create data pages.
pub struct DataPageBuilder {
    base: PageBuilder,
    offsets_len: usize,
    payload_size: usize,
}

// TODO: Optimizes the page layout with
// https://cseweb.ucsd.edu//~csjgwang/pubs/ICDE17_BwTree.pdf
impl DataPageBuilder {
    pub fn new(is_leaf: bool) -> Self {
        Self {
            base: PageBuilder::new(PageKind::Data, is_leaf),
            offsets_len: 0,
            payload_size: 0,
        }
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
        (self.offsets_len + 1) * size_of::<u32>() + self.payload_size
    }

    pub fn build<A>(self, alloc: &A) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        let ptr = self.base.build(alloc, self.size());
        ptr.map(|ptr| unsafe {
            DataPageBuf::new(ptr, self);
            ptr
        })
    }

    pub fn build_from_iter<A, I>(mut self, alloc: &A, iter: &mut I) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
        I: RewindableIter,
        I::Key: Encodable,
        I::Value: Encodable,
    {
        iter.rewind();
        while let Some((key, value)) = iter.next() {
            self.add(key, value);
        }
        let ptr = self.base.build(alloc, self.size());
        ptr.map(|ptr| unsafe {
            let mut buf = DataPageBuf::new(ptr, self);
            iter.rewind();
            while let Some((key, value)) = iter.next() {
                buf.add(key, value);
            }
            ptr
        })
    }
}

struct DataPageBuf {
    offsets: *mut u32,
    payload: BufWriter,
    current: usize,
}

impl DataPageBuf {
    unsafe fn new(mut ptr: PagePtr, builder: DataPageBuilder) -> Self {
        let content = ptr.content_mut() as *mut u32;
        content.write(builder.offsets_len as u32);
        let offsets = content.add(1);
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

/// An immutable reference to a data page.
pub struct DataPageRef<'a, K, V> {
    base: PageRef<'a>,
    offsets: &'a [u32],
    payload: *const u8,
    _mark: PhantomData<(K, V)>,
}

impl<'a, K, V> DataPageRef<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    pub unsafe fn new(base: PageRef<'a>) -> Self {
        let content = base.content() as *const u32;
        let offsets_len = content.read() as usize;
        let offsets_ptr = content.add(1);
        let offsets = slice::from_raw_parts(offsets_ptr, offsets_len);
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

    // Returns the first entry that is no greater than the target.
    pub fn seek_back(&self, target: &K) -> Option<(K, V)> {
        let index = self.rank(target);
        for i in (0..=index).rev() {
            if let Some((key, value)) = self.index(i) {
                if &key <= target {
                    return Some((key, value));
                }
            }
        }
        None
    }

    pub fn iter(&self) -> DataPageIter<'a, K, V> {
        DataPageIter::new(self.clone())
    }

    pub fn into_iter(self) -> DataPageIter<'a, K, V> {
        DataPageIter::new(self)
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

impl<'a, K, V> Clone for DataPageRef<'a, K, V> {
    fn clone(&self) -> Self {
        Self {
            base: self.base,
            offsets: self.offsets,
            payload: self.payload,
            _mark: PhantomData,
        }
    }
}

impl<'a, K, V> Deref for DataPageRef<'a, K, V> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'a, K, V> From<DataPageRef<'a, K, V>> for PageRef<'a> {
    fn from(page: DataPageRef<'a, K, V>) -> Self {
        page.base
    }
}

pub struct DataPageIter<'a, K, V> {
    page: DataPageRef<'a, K, V>,
    next: usize,
    current: Option<(K, V)>,
}

impl<'a, K, V> DataPageIter<'a, K, V>
where
    K: Decodable,
    V: Decodable,
{
    pub fn new(page: DataPageRef<'a, K, V>) -> Self {
        Self {
            page,
            next: 0,
            current: None,
        }
    }
}

impl<'a, K, V> ForwardIter for DataPageIter<'a, K, V>
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

impl<'a, K, V> SeekableIter for DataPageIter<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    fn seek(&mut self, target: &K) {
        self.next = self.page.rank(target);
        self.current = None;
    }
}

impl<'a, K, V> RewindableIter for DataPageIter<'a, K, V>
where
    K: Decodable + Ord,
    V: Decodable,
{
    fn rewind(&mut self) {
        self.next = 0;
        self.current = None;
    }
}
