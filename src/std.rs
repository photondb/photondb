use std::{
    future::Future,
    ops::Deref,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use futures::task::noop_waker_ref;

use crate::{env::Std, raw, Options, Result};

pub struct Table(raw::Table<Std>);

impl Table {
    pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let table = poll(raw::Table::open(Std, path, options))?;
        Ok(Self(table))
    }

    pub fn close(self) {
        poll(self.0.close());
    }

    pub fn get<F, R>(&self, key: &[u8], lsn: u64, f: F) -> Result<R>
    where
        F: FnOnce(Option<&[u8]>) -> R,
    {
        poll(self.0.get(key, lsn, f))
    }

    pub fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        poll(self.0.put(key, lsn, value))
    }

    pub fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        poll(self.0.delete(key, lsn))
    }
}

impl Deref for Table {
    type Target = raw::Table<Std>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn poll<F: Future>(mut future: F) -> F::Output {
    let cx = &mut Context::from_waker(noop_waker_ref());
    let fut = unsafe { Pin::new_unchecked(&mut future) };
    match fut.poll(cx) {
        Poll::Ready(output) => output,
        Poll::Pending => unreachable!(),
    }
}
