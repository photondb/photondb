use std::{mem::size_of, slice};

// An unsafe, little-endian buffer reader.
pub struct BufReader {
    ptr: *const u8,
    pos: usize,
}

macro_rules! get_int {
    ($name:ident, $t:ty) => {
        pub unsafe fn $name(&mut self) -> $t {
            let ptr = self.ptr.add(self.pos) as *const $t;
            self.pos += size_of::<$t>();
            ptr.read().to_le()
        }
    };
}

impl BufReader {
    pub fn new(ptr: *const u8) -> Self {
        Self { ptr, pos: 0 }
    }

    pub const fn pos(&self) -> usize {
        self.pos
    }

    get_int!(get_u8, u8);
    get_int!(get_u16, u16);
    get_int!(get_u32, u32);
    get_int!(get_u64, u64);

    pub unsafe fn get_slice<'a>(&mut self, len: usize) -> &'a [u8] {
        let ptr = self.ptr.add(self.pos);
        self.pos += len;
        slice::from_raw_parts(ptr, len)
    }

    pub unsafe fn get_length_prefixed_slice<'a>(&mut self) -> &'a [u8] {
        let len = self.get_u32();
        self.get_slice(len as usize)
    }
}

// An unsafe, little-endian buffer writer.
pub struct BufWriter {
    ptr: *mut u8,
    pos: usize,
}

macro_rules! put_int {
    ($name:ident, $t:ty) => {
        pub unsafe fn $name(&mut self, v: $t) {
            let ptr = self.ptr.add(self.pos) as *mut $t;
            ptr.write(v.to_le());
            self.pos += size_of::<$t>();
        }
    };
}

impl BufWriter {
    pub fn new(ptr: *mut u8) -> Self {
        Self { ptr, pos: 0 }
    }

    pub const fn pos(&self) -> usize {
        self.pos
    }

    put_int!(put_u8, u8);
    put_int!(put_u16, u16);
    put_int!(put_u32, u32);
    put_int!(put_u64, u64);

    pub unsafe fn put_slice(&mut self, slice: &[u8]) {
        let ptr = self.ptr.add(self.pos) as *mut u8;
        ptr.copy_from(slice.as_ptr(), slice.len());
        self.pos += slice.len();
    }

    pub unsafe fn put_length_prefixed_slice(&mut self, slice: &[u8]) {
        self.put_u32(slice.len() as u32);
        self.put_slice(slice);
    }

    pub const fn length_prefixed_slice_size(slice: &[u8]) -> usize {
        size_of::<u32>() + slice.len()
    }
}
