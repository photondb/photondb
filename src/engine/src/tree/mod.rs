mod error;
use error::{Error, Result};

mod btree;

struct PageRef<'a>(&'a [u8]);

impl<'a> PageRef<'a> {
    fn next(self) {
        todo!()
    }

    fn kind(self) -> PageKind {
        todo!()
    }

    fn length(self) -> u8 {
        todo!()
    }

    fn version(self) -> u64 {
        todo!()
    }
}

#[repr(u8)]
enum PageKind {
    Base = 0,
}

enum PagePtr<'a> {
    Mem(PageRef<'a>),
    Disk(u64),
}

const MEM_DISK_MASK: u64 = 1 << 63;

impl<'a> From<u64> for PagePtr<'a> {
    fn from(ptr: u64) -> Self {
        if ptr & MEM_DISK_MASK == 0 {
            todo!()
        } else {
            todo!()
        }
    }
}

impl<'a> Into<u64> for PagePtr<'a> {
    fn into(self) -> u64 {
        todo!()
    }
}
