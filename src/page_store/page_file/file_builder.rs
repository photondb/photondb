use std::collections::{HashMap, HashSet};

use futures::Future;
use photonio::io::{Write, WriteExt};

use crate::page_store::Result;

const IO_BUFFER_SIZE: u64 = 8192 * 4;

/// Builder for a page file.
///
/// File format:
///
/// File = {data blocks} {meta blocks} {index blocks} {footer}
/// data blocks = [{data block}] --- one block per tree page
/// meta blocks = {page table block} {delete pages block}
/// page table block = [(page_id, page_addr[low 32bit])]
/// delete pages block = [delete-page-addr]
/// index_blocks = {data block index} {meta block index}
/// data block index = {page_addr[low 32bit], file_offset}
/// meta block index = {file_offset}
/// footer = {magic_number} {data block index} {meta block index} {checksum}
///
/// `page_addr`'s high 32 bit always be `file-id`(`PageAddr = {file id} {write
/// buffer index}`), so it only store lower 32 bit.
pub(crate) struct FileBuilder<W: Write> {
    writer: BufferWriter<W>,

    index: IndexBlockBuilder,
    meta: MetaBlockBuilder,
}

impl<W: Write> FileBuilder<W> {
    /// Create new file builder for given writer.
    pub(crate) fn new(writer: W) -> Self {
        let writer = BufferWriter::new(writer, IO_BUFFER_SIZE);
        Self {
            writer,
            index: Default::default(),
            meta: Default::default(),
        }
    }

    /// Add a new page to builder.
    pub(crate) async fn add_page(
        &mut self,
        page_id: u64,
        page_addr: u64,
        page_content: &[u8],
    ) -> Result<()> {
        let file_offset = self.writer.write(page_content).await?;
        self.index.add_data_block(page_addr, file_offset);
        self.meta.add_page(page_id, page_addr);
        Ok(())
    }

    /// Add delete page to builder.
    pub(crate) fn add_delete_pages(&mut self, page_addrs: &[u64]) {
        self.meta.delete_pages(page_addrs)
    }

    // Finish build page file.
    pub(crate) async fn finish(&mut self) -> Result<()> {
        self.finish_meta_block().await?;
        self.finish_file_footer().await?;
        self.writer.flush().await
    }
}

impl<W: Write> FileBuilder<W> {
    async fn finish_meta_block(&mut self) -> Result<()> {
        {
            let page_tables = self.meta.finish_page_table_block();
            let file_offset = self.writer.write(&page_tables).await?;
            self.index
                .add_meta_block(MetaBlockKind::PageTable, file_offset)
        }
        {
            let delete_pages = self.meta.finish_delete_pages_block();
            let file_offset = self.writer.write(&delete_pages).await?;
            self.index
                .add_meta_block(MetaBlockKind::DeletePages, file_offset)
        };
        Ok(())
    }

    async fn finish_file_footer(&self) -> Result<()> {
        let (data_index, meta_index) = self.index.finish_index_block();
        // TODO: build footer & checksum and write into self.writer.
        Ok(())
    }
}

struct BufferWriter<W: Write> {
    writer: W,
    offset: u64,
    // TODO: buffer
}

impl<W: Write> BufferWriter<W> {
    fn new(writer: W, buf_size: u64) -> Self {
        Self { writer, offset: 0 }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<u64> {
        let offset = self.offset;
        self.offset += buf.len() as u64;
        todo!();
        Ok(offset)
    }

    async fn flush(&mut self) -> Result<()> {
        todo!()
    }
}

#[derive(Default)]
struct MetaBlockBuilder {
    delete_page_addrs: HashSet<u64>,
    page_table: HashMap<u64, u64>,
}

impl MetaBlockBuilder {
    pub fn add_page(&mut self, page_id: u64, page_addr: u64) {
        self.page_table.insert(page_id, page_addr);
    }

    pub fn delete_pages(&mut self, page_addrs: &[u64]) {
        for page_addr in page_addrs {
            self.delete_page_addrs.insert(*page_addr);
        }
    }

    pub fn finish_page_table_block(&self) -> Vec<u8> {
        todo!()
    }

    pub fn finish_delete_pages_block(&self) -> Vec<u8> {
        todo!()
    }
}

#[derive(Default)]
struct IndexBlockBuilder {}

impl IndexBlockBuilder {
    #[inline]
    pub fn add_data_block(&mut self, page_addr: u64, file_offset: u64) {
        todo!()
    }

    #[inline]
    pub fn add_meta_block(&mut self, kind: MetaBlockKind, file_offset: u64) {
        todo!()
    }

    pub fn finish_index_block(
        &self,
    ) -> (
        BlockHandler, /* data index */
        BlockHandler, /* meta index */
    ) {
        todo!()
    }
}

enum MetaBlockKind {
    PageTable,
    DeletePages,
}

struct BlockHandler {
    offset: u64,
    length: u64,
}
