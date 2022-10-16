use super::PageEpoch;

pub(crate) struct Key<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) lsn: u64,
}

impl<'a> Key<'a> {
    pub(crate) fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
    }
}

pub(crate) struct Entry<'a> {
    pub(crate) key: Key<'a>,
    pub(crate) kind: EntryKind,
    pub(crate) value: &'a [u8],
}

impl<'a> Entry<'a> {
    pub(crate) fn put(key: Key<'a>, value: &'a [u8]) -> Self {
        Self {
            key,
            kind: EntryKind::Put,
            value,
        }
    }

    pub(crate) fn delete(key: Key<'a>) -> Self {
        Self {
            key,
            kind: EntryKind::Delete,
            value: &[],
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum EntryKind {
    Put,
    Delete,
}

#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct Index {
    pub(crate) id: u64,
    pub(crate) epoch: PageEpoch,
}

#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct Range<'a> {
    pub(crate) left: &'a [u8],
    pub(crate) right: Option<&'a [u8]>,
}
