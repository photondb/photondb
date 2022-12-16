/// A simple fixed size bitmap implementation.
#[derive(Clone)]
pub(crate) struct FixedBitmap {
    cap: u32,
    len: u32,
    bits: Vec<u64>,
}

pub(crate) struct FixedBitmapIter<'a> {
    bitmap: &'a FixedBitmap,
    key: usize,
    value: u64,
}

impl FixedBitmap {
    pub(crate) fn new(cap: u32) -> Self {
        let size = match cap % 64 {
            0 => cap / 64,
            _ => cap / 64 + 1,
        };
        let bits = vec![0u64; size as usize];
        FixedBitmap { cap, len: 0, bits }
    }

    /// Set the corresponding bit.
    pub(crate) fn set(&mut self, index: u32) -> bool {
        let (key, bit) = (key(index), bit(index));
        let old_w = self.bits[key];
        let new_w = old_w | 1 << bit;
        let inserted = (old_w ^ new_w) >> bit; // 1 or 0
        self.bits[key] = new_w;
        self.len += inserted as u32;
        inserted != 0
    }

    /// Clear the corresponding bit.
    #[allow(unused)]
    pub(crate) fn clear(&mut self, index: u32) -> bool {
        let (key, bit) = (key(index), bit(index));
        let old_w = self.bits[key];
        let new_w = old_w & !(1 << bit);
        let removed = (old_w ^ new_w) >> bit; // 0 or 1
        self.bits[key] = new_w;
        self.len -= removed as u32;
        removed != 0
    }

    /// Test whether the specified bit is set.
    pub(crate) fn test(&self, index: u32) -> bool {
        let (key, bit) = (key(index), bit(index));
        self.bits[key] & (1 << bit) != 0
    }

    /// Returns the number of set bits.
    #[inline]
    #[allow(unused)]
    pub(crate) fn len(&self) -> u32 {
        self.len
    }

    /// Returns the total bits.
    #[inline]
    #[allow(unused)]
    pub(crate) fn cap(&self) -> u32 {
        self.cap
    }

    /// Returns the number of unset bits.
    #[inline]
    pub(crate) fn free(&self) -> u32 {
        self.cap
            .checked_sub(self.len)
            .expect("The len does not exceed the capacity")
    }

    #[inline]
    pub(crate) fn is_full(&self) -> bool {
        self.len == self.cap
    }

    #[inline]
    #[allow(unused)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    #[allow(unused)]
    pub(crate) fn iter(&self) -> FixedBitmapIter {
        FixedBitmapIter::new(self)
    }
}

impl<'a> FixedBitmapIter<'a> {
    fn new(bitmap: &'a FixedBitmap) -> Self {
        FixedBitmapIter {
            bitmap,
            key: 0,
            value: bitmap.bits[0],
        }
    }
}

impl<'a> Iterator for FixedBitmapIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.value == 0 {
                self.key += 1;
                if let Some(value) = self.bitmap.bits.get(self.key) {
                    self.value = *value;
                    continue;
                } else {
                    return None;
                }
            }

            let index = self.value.trailing_zeros() as usize;
            self.value &= self.value - 1;
            return Some((64 * self.key + index) as u32);
        }
    }
}

#[inline]
fn key(index: u32) -> usize {
    index as usize / 64
}

#[inline]
fn bit(index: u32) -> usize {
    index as usize % 64
}
