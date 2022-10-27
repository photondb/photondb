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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Index {
    pub(crate) id: u64,
    pub(crate) epoch: u64,
}

impl Index {
    pub(crate) const fn new(id: u64, epoch: u64) -> Self {
        Self { id, epoch }
    }
}
