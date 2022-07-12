use super::{RandomRead, Result, SequentialWrite};

struct Footer {
    pub meta_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub magic_number: u64,
}

impl Footer {
    const ENCODED_SIZE: usize = 40;
}

struct BlockHandle {
    pub offset: u64,
    pub size: u64,
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

pub struct PageStore {}

impl PageStore {
    pub async fn open(path: &str) -> Result<Self> {
        Ok(Self {})
    }

    pub async fn read_page(&self, addr: PageAddr) -> Result<Vec<u8>> {
        todo!()
    }

    pub async fn write_page(&self, id: u64, page: &[u8]) -> Result<PageAddr> {
        todo!()
    }
}

struct PageFile {}

impl PageFile {
    pub fn read_page(&self, index: usize) -> Result<Vec<u8>> {
        todo!()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct PageAddr {
    // The file number.
    pub file: u32,
    // The index of the page in the file.
    pub index: u16,
}
