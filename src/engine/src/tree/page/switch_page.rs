use super::*;
use crate::util::{BufReader, BufWriter};

pub struct SwitchPageBuilder {
    base: PageBuilder,
}

impl Default for SwitchPageBuilder {
    fn default() -> Self {
        Self {
            base: PageBuilder::new(PageKind::Switch),
        }
    }
}

impl SwitchPageBuilder {
    pub fn build<A>(self, alloc: &A, old_addr: u64, new_addr: u64) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        let ptr = self.base.build(alloc, 16);
        ptr.map(|mut ptr| unsafe {
            let mut buf = BufWriter::new(ptr.content_mut());
            buf.put_u64(old_addr);
            buf.put_u64(new_addr);
            ptr
        })
    }
}

#[derive(Clone)]
pub struct SwitchPageRef<'a>(PageRef<'a>);

impl<'a> SwitchPageRef<'a> {
    pub fn new(base: PageRef<'a>) -> Self {
        assert_eq!(base.kind(), PageKind::Switch);
        Self(base)
    }

    pub fn old_addr(&self) -> u64 {
        unsafe {
            let mut buf = BufReader::new(self.0.content());
            buf.get_u64()
        }
    }

    pub fn new_addr(&self) -> u64 {
        unsafe {
            let mut buf = BufReader::new(self.0.content().add(8));
            buf.get_u64()
        }
    }
}

impl<'a> From<PageRef<'a>> for SwitchPageRef<'a> {
    fn from(base: PageRef<'a>) -> Self {
        Self::new(base)
    }
}
