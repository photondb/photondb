use std::{collections::BTreeMap, sync::Arc};

use photonio::io::{ReadAt, ReadAtExt};

use super::{
    file_builder::{self, DeletePages, Footer, IndexBlock, PageTable},
    types::FileMeta,
};
use crate::{
    page,
    page_store::{Error, Result},
};

pub(crate) struct FileReader<R: ReadAt> {
    reader: R,
    file_meta: Arc<FileMeta>,
}

impl<R: ReadAt> FileReader<R> {
    /// Open page file reader for given reader.
    pub(crate) async fn open(reader: R, file_size: u32, file_id: u32) -> Result<Self> {
        let file_meta = Self::open_file_meta(&reader, file_size, file_id).await?;
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
    pub(crate) async fn read_page_table(&self) -> Result<BTreeMap<u64, u64>> {
        let (page_table_offset, page_table_len) = self.file_meta.get_page_table_meta_page()?;
        let mut buf = vec![0u8; page_table_len];
        self.reader
            .read_exact_at(&mut buf, page_table_offset)
            .await
            .expect("read page table meta page fail");
        let table = PageTable::decode(&buf)?;
        Ok(table.into())
    }

    /// Returns the delete page addrs in the file.
    pub(crate) async fn read_delete_pages(&self) -> Result<Vec<u64> /* delete page addrs */> {
        let (del_offset, del_len) = self.file_meta.get_delete_pages_meta_page()?;
        let mut buf = vec![0u8; del_len];
        self.reader
            .read_exact_at(&mut buf, del_offset)
            .await
            .expect("read delete pages meta page fail");
        let dels = DeletePages::decode(&buf)?;
        Ok(dels.into())
    }

    /// Reads the exact number of bytes from the page specified by `page_addr`
    /// to fill `buf` the read page size is same as the provided
    /// `buf.len()`.
    pub(crate) async fn read_page(&self, page_addr: u64, buf: &mut [u8]) -> Result<()> {
        let (offset, page_size) = self
            .file_meta
            .get_page_handle(page_addr)
            .ok_or(Error::InvalidArgument)?;
        if buf.len() != page_size {
            return Err(Error::InvalidArgument);
        }

        self.reader
            .read_exact_at(buf, offset)
            .await
            .expect("read page data fail");

        Ok(())
    }
}

impl<R: ReadAt> FileReader<R> {
    pub(crate) async fn open_file_meta(
        reader: &R,
        file_size: u32,
        file_id: u32,
    ) -> Result<Arc<FileMeta>> {
        let footer = Self::read_footer(reader, file_size).await?;
        let (mut indexes, offsets) = Self::read_index_block(reader, footer).await?;
        Ok(Arc::new(FileMeta::new(
            file_id, file_size, indexes, offsets,
        )))
    }

    async fn read_footer(read: &R, file_size: u32) -> Result<Footer> {
        if file_size <= Footer::size() {
            return Err(Error::Corrupted);
        }
        let footer_offset = (file_size - Footer::size()) as u64;
        let mut buf = vec![0u8; Footer::size() as usize];
        read.read_exact_at(&mut buf, footer_offset)
            .await
            .expect("read file footer error");
        let footer = Footer::decode(&buf)?;
        Ok(footer)
    }

    async fn read_index_block(
        read: &R,
        footer: Footer,
    ) -> Result<(
        Vec<u64>,           /* meta_idx */
        BTreeMap<u64, u64>, /* data_offsets */
    )> {
        let mut data_idx_bytes = vec![0u8; footer.data_handle.length as usize];
        read.read_exact_at(&mut data_idx_bytes, footer.data_handle.offset)
            .await
            .expect("read data page index fail");

        let mut meta_idx_bytes = vec![0u8; footer.meta_handle.length as usize];
        read.read_exact_at(&mut meta_idx_bytes, footer.meta_handle.offset)
            .await
            .expect("read meta page index fail");

        let IndexBlock {
            page_offsets,
            meta_page_table,
            meta_delete_pages,
        } = IndexBlock::decode(&data_idx_bytes, &meta_idx_bytes)?;

        let mut indexes = Vec::with_capacity(3);
        indexes.push(meta_page_table.as_ref().unwrap().to_owned());
        indexes.push(meta_delete_pages.as_ref().unwrap().to_owned());
        indexes.push(footer.data_handle.offset); // meta block's end is index_block's start.

        Ok((indexes, page_offsets))
    }
}
