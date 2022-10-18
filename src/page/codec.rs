use std::{mem, slice};

/// Encodes an object to a buffer.
pub(crate) trait EncodeTo {
    /// Returns the exact size to encode this object.
    fn encode_size(&self) -> usize;

    /// Encodes this object to a [`BufWriter`].
    ///
    /// # Safety
    ///
    /// The `BufWriter` must be initialized with enough space to encode this object.
    unsafe fn encode_to(&self, w: &mut BufWriter);
}

/// Decodes an object from a buffer.
pub(crate) trait DecodeFrom {
    /// Decodes an object from a [`BufReader`].
    ///
    /// # Safety
    ///
    /// The `BufReader` must be initialized with enough data to decode such an object.
    unsafe fn decode_from(r: &mut BufReader) -> Self;
}

/// An unsafe, little-endian buffer reader.
pub(crate) struct BufReader(*const u8);

macro_rules! get_int {
    ($name:ident, $t:ty) => {
        pub(super) unsafe fn $name(&mut self) -> $t {
            let mut v: $t = 0;
            let ptr = &mut v as *mut $t as *mut u8;
            let len = mem::size_of::<$t>();
            self.0.copy_to_nonoverlapping(ptr, len);
            self.skip(len);
            <$t>::from_le(v)
        }
    };
}

impl BufReader {
    pub(super) const fn new(ptr: *const u8) -> Self {
        Self(ptr)
    }

    pub(super) unsafe fn skip(&mut self, n: usize) {
        self.0 = self.0.add(n);
    }

    get_int!(get_u8, u8);
    get_int!(get_u32, u32);
    get_int!(get_u64, u64);

    pub(super) unsafe fn get_slice<'a>(&mut self, len: usize) -> &'a [u8] {
        let ptr = self.0;
        self.skip(len);
        slice::from_raw_parts(ptr, len)
    }

    pub(super) unsafe fn get_length_prefixed_slice<'a>(&mut self) -> &'a [u8] {
        let len = self.get_u32();
        self.get_slice(len as usize)
    }
}

/// An unsafe, little-endian buffer writer.
pub(crate) struct BufWriter(*mut u8);

macro_rules! put_int {
    ($name:ident, $t:ty) => {
        pub(super) unsafe fn $name(&mut self, v: $t) {
            let v = v.to_le();
            let ptr = &v as *const $t as *const u8;
            let len = mem::size_of::<$t>();
            self.0.copy_from_nonoverlapping(ptr, len);
            self.skip(len);
        }
    };
}

impl BufWriter {
    pub(super) const fn new(ptr: *mut u8) -> Self {
        Self(ptr)
    }

    pub(super) unsafe fn skip(&mut self, n: usize) {
        self.0 = self.0.add(n);
    }

    pub(super) unsafe fn offset_from(&self, origin: *const u8) -> isize {
        self.0.offset_from(origin)
    }

    put_int!(put_u8, u8);
    put_int!(put_u32, u32);
    put_int!(put_u64, u64);

    pub(super) unsafe fn put_slice(&mut self, slice: &[u8]) {
        self.0.copy_from_nonoverlapping(slice.as_ptr(), slice.len());
        self.skip(slice.len());
    }

    pub(super) unsafe fn put_length_prefixed_slice(&mut self, slice: &[u8]) {
        assert!(slice.len() <= u32::MAX as usize);
        self.put_u32(slice.len() as u32);
        self.put_slice(slice);
    }

    pub(super) const fn length_prefixed_slice_size(slice: &[u8]) -> usize {
        mem::size_of::<u32>() + slice.len()
    }
}
