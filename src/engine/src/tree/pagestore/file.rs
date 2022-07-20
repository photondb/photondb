use std::io::Result;

use super::env::{RandomRead, SequentialWrite};

struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

struct PageFileFooter {
    pub meta_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub magic_number: u64,
}

impl PageFileFooter {
    const ENCODED_SIZE: usize = 40;
}

struct PageFileReader<R> {
    file: R,
}

impl<R: RandomRead> PageFileReader<R> {
    pub fn open(file: R, file_size: u64) -> Result<Self> {
        todo!()
    }

    pub fn read_page(&mut self) -> Result<Vec<u8>> {
        todo!()
    }
}

struct PageFileWriter<W> {
    file: W,
}

impl<W: SequentialWrite> PageFileWriter<W> {
    pub fn open(file: W) -> Result<Self> {
        todo!()
    }

    pub fn add_page(&mut self) {}

    pub fn add_obsolete_page(&mut self) {}
}

struct PageFile {}

impl PageFile {
    pub fn read_page(&self, index: usize) -> Result<Vec<u8>> {
        todo!()
    }
}
