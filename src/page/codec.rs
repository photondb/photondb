use std::{mem, slice};

/// Encodes an object.
pub(crate) trait EncodeTo {
    /// Returns the exact size to encode the object.
    fn encode_size(&self) -> usize;

    /// Encodes the object to the encoder.
    ///
    /// # Safety
    ///
    /// The encoder must have enough space to encode the object.
    unsafe fn encode_to(&self, encoder: &mut Encoder);
}

/// Decodes an object.
pub(crate) trait DecodeFrom {
    /// Decodes an object from the decoder.
    ///
    /// # Safety
    ///
    /// The decoder must have enough data to decode the object.
    unsafe fn decode_from(decoder: &mut Decoder) -> Self;
}

// An unsafe, little-endian encoder.
pub(crate) struct Encoder {
    buf: *mut u8,
    len: usize,
    cursor: *mut u8,
}

macro_rules! put_int {
    ($name:ident, $t:ty) => {
        pub(super) unsafe fn $name(&mut self, v: $t) {
            let v = v.to_le();
            let ptr = &v as *const $t as *const u8;
            let len = mem::size_of::<$t>();
            self.copy_from(ptr, len);
        }
    };
}

impl Encoder {
    pub(super) fn new(buf: &mut [u8]) -> Self {
        Self {
            buf: buf.as_mut_ptr(),
            len: buf.len(),
            cursor: buf.as_mut_ptr(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) unsafe fn offset(&self) -> usize {
        self.cursor.offset_from(self.buf) as _
    }

    pub(super) unsafe fn remaining(&self) -> usize {
        self.len() - self.offset()
    }

    unsafe fn advance(&mut self, n: usize) {
        self.cursor = self.cursor.add(n);
    }

    unsafe fn copy_from(&mut self, ptr: *const u8, len: usize) {
        debug_assert!(len <= self.remaining());
        self.cursor.copy_from_nonoverlapping(ptr, len);
        self.advance(len);
    }

    put_int!(put_u8, u8);
    put_int!(put_u32, u32);
    put_int!(put_u64, u64);

    pub(super) unsafe fn put_slice(&mut self, v: &[u8]) {
        self.copy_from(v.as_ptr(), v.len());
    }
}

// An unsafe, little-endian decoder.
pub(crate) struct Decoder {
    buf: *const u8,
    len: usize,
    cursor: *const u8,
}

macro_rules! get_int {
    ($name:ident, $t:ty) => {
        pub(super) unsafe fn $name(&mut self) -> $t {
            let mut v: $t = 0;
            let ptr = &mut v as *mut $t as *mut u8;
            let len = mem::size_of::<$t>();
            debug_assert!(len <= self.remaining());
            self.cursor.copy_to_nonoverlapping(ptr, len);
            self.advance(len);
            <$t>::from_le(v)
        }
    };
}

impl Decoder {
    pub(super) fn new(buf: &[u8]) -> Self {
        Self {
            buf: buf.as_ptr(),
            len: buf.len(),
            cursor: buf.as_ptr(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) unsafe fn offset(&self) -> usize {
        self.cursor.offset_from(self.buf) as _
    }

    pub(super) unsafe fn remaining(&self) -> usize {
        self.len() - self.offset()
    }

    unsafe fn advance(&mut self, len: usize) {
        self.cursor = self.cursor.add(len);
    }

    get_int!(get_u8, u8);
    get_int!(get_u32, u32);
    get_int!(get_u64, u64);

    pub(super) unsafe fn get_slice<'a>(&mut self, len: usize) -> &'a [u8] {
        debug_assert!(len <= self.remaining());
        let buf = slice::from_raw_parts(self.cursor, len);
        self.advance(len);
        buf
    }
}
