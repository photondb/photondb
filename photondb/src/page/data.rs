use std::cmp::Ordering;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Key<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) lsn: u64,
}

impl<'a> Key<'a> {
    pub(crate) const fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
    }
}

impl Ord for Key<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Order by the raw key ascendingly and the LSN descendingly.
        match self.raw.cmp(other.raw) {
            Ordering::Equal => other.lsn.cmp(&self.lsn),
            o => o,
        }
    }
}

impl PartialOrd for Key<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Range<'a> {
    pub(crate) start: &'a [u8],
    pub(crate) end: Option<&'a [u8]>,
}

impl<'a> Range<'a> {
    /// Returns a range that covers all keys.
    pub(crate) const fn full() -> Self {
        Self {
            start: [].as_slice(),
            end: None,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

impl<'a> Value<'a> {
    /// Returns the length of value.
    pub(crate) fn len(&self) -> usize {
        match self {
            Value::Put(v) => v.len(),
            Value::Delete => 0,
        }
    }
}

/// An index to a child page.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Index {
    /// The page id of the child.
    pub(crate) id: u64,
    /// The page epoch of the child.
    pub(crate) epoch: u64,
}

impl Index {
    pub(crate) const fn new(id: u64, epoch: u64) -> Self {
        Self { id, epoch }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_ord() {
        let a = Key::new(b"foo", 1);
        let b = Key::new(b"foo", 2);
        let c = Key::new(b"bar", 1);
        let d = Key::new(b"bar", 2);
        assert!(a > b);
        assert!(a > c);
        assert!(a > d);
        assert!(b > c);
        assert!(b > d);
        assert!(c > d);
    }
}
