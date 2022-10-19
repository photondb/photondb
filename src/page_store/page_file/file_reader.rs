use std::sync::Arc;

use photonio::{fs::File, io::ReadAt};

use super::types::FileMeta;
use crate::page_store::Result;

pub(crate) struct FileReader<R: ReadAt> {
    reader: R,
    file_meta: Arc<FileMeta>,
}

impl<R: ReadAt> FileReader<R> {
    /// Open page file reader for given reader.
    pub(crate) fn open(reader: R, file_size: u32, file_id: u32) -> Result<Self> {
        let file_meta = Arc::new(FileMeta::new(file_id, file_size, vec![], vec![]));
        Self::open_with_meta(reader, file_meta)
    }

    // Open page file reader with exist `file_meta`(e.g. version.active_files.meta).
    pub(crate) fn open_with_meta(reader: R, file_meta: Arc<FileMeta>) -> Result<Self> {
        Ok(Self { reader, file_meta })
    }

    //// Returns the file metadata for current reader.
    pub(crate) fn file_metadata(&self) -> Arc<FileMeta> {
        self.file_meta.clone()
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

    /// Reads the exact number of bytes from the page specified by `page_addr`
    /// to fill `buf` the read page size is same as the provided
    /// `buf.len()`.
    pub(crate) async fn read_page(&self, page_addr: u64, buf: &mut [u8]) -> Result<()> {
        todo!()
    }
}
