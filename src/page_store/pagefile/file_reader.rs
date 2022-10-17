use photonio::io::ReadAt;

use crate::page_store::Result;

pub(crate) struct FileReader<R: ReadAt> {
    reader: R,
}

impl<R: ReadAt> FileReader<R> {
    /// Open page file reader for given reader.
    /// it will prepare and load index block in memory.
    pub(crate) fn open(reader: R, _file_size: usize) -> Result<Self> {
        // TODO: load index block
        Ok(Self { reader })
    }

    /// Returns the page table in the file.
    pub(crate) async fn read_page_table(
        &self,
    ) -> Result<Vec<(u64 /* page_id */, u64 /* page_addr */)>> {
        todo!()
    }

    /// Returns the delete page addrs in the file.
    pub(crate) async fn read_delete_pages(&self) -> Result<Vec<u64> /* delete page addrs */> {
        todo!()
    }

    /// Reads the exact number of bytes from the page specified by `page_addr` to fill `buf`
    /// the read page size is same as the provided `buf.len()`.
    pub(crate) async fn read_page(&self, page_addr: u64, buf: &mut [u8]) -> Result<()> {
        todo!()
    }
}
