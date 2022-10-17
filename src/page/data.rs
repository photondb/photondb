#[derive(Clone, Debug)]
pub(crate) struct Key<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) lsn: u64,
}

impl<'a> Key<'a> {
    pub(crate) fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Index {
    pub(crate) id: u64,
    pub(crate) epoch: u64,
}

impl Index {
    pub(crate) fn new(id: u64) -> Self {
        Self { id, epoch: 0 }
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct Range<'a> {
    pub(crate) left: &'a [u8],
    pub(crate) right: Option<&'a [u8]>,
}
