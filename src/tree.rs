use crate::{Error, Options, Result};

struct PageStore;

struct Txn;

impl PageStore {
    fn begin(&self) -> Txn {
        Txn
    }
}

pub(crate) struct Key<'a> {
    raw: &'a [u8],
    lsn: u64,
}

impl<'a> Key<'a> {
    pub(crate) fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
    }
}

pub(crate) struct Entry<'a> {
    key: Key<'a>,
    kind: EntryKind,
    value: &'a [u8],
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

#[derive(Copy, Clone)]
pub(crate) enum EntryKind {
    Put,
    Delete,
}

pub(crate) struct Tree {
    store: PageStore,
}

impl Tree {
    pub(crate) async fn open(options: Options) -> Result<Self> {
        todo!()
    }

    pub(crate) async fn get<F, R>(&self, key: Key<'_>, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let txn = self.store.begin();
        loop {
            match self.try_get(&txn, &key).await {
                Ok(value) => return Ok(f(value)),
                Err(Error::Conflicted) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) async fn write(&self, entry: Entry<'_>) -> Result<()> {
        let mut txn = self.store.begin();
        loop {
            match self.try_write(&mut txn).await {
                Ok(()) => return Ok(()),
                Err(Error::Conflicted) => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

impl Tree {
    async fn try_get(&self, txn: &Txn, key: &Key<'_>) -> Result<Option<&[u8]>> {
        todo!()
    }

    async fn try_write(&self, txn: &mut Txn) -> Result<()> {
        todo!()
    }
}
