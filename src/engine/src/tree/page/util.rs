use std::mem::size_of;

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
