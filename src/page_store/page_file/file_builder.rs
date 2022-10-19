use std::collections::{HashMap, HashSet};

use photonio::{fs::File, io::WriteExt};

use crate::page_store::Result;

const IO_BUFFER_SIZE: usize = 4096 * 4;

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
pub(crate) struct FileBuilder {
    writer: BufferedWriter,

    index: IndexBlockBuilder,
    meta: MetaBlockBuilder,
}

impl FileBuilder {
    /// Create new file builder for given writer.
    pub(crate) fn new(file: File, use_direct: bool) -> Self {
        let writer = BufferedWriter::new(file, IO_BUFFER_SIZE, use_direct);
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
        self.writer.flush_and_sync().await
    }
}

impl FileBuilder {
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

const ALIGN_SIZE: usize = 4096;
const ALIGN_MASK: u64 = 0xffff_f000;

struct BufferedWriter {
    file: File,

    use_direct_io: bool,

    next_page_offset: u64,
    actual_data_size: usize,

    buffer: Vec<u8>,
}

impl BufferedWriter {
    fn new(file: File, io_batch_size: usize, use_direct_io: bool) -> Self {
        Self {
            file,
            use_direct_io,
            next_page_offset: 0,
            actual_data_size: 0,
            buffer: Self::alloc_buffer(io_batch_size, use_direct_io),
        }
    }

    fn alloc_buffer(n: usize, use_direct_io: bool) -> Vec<u8> {
        if !use_direct_io {
            return Vec::with_capacity(n);
        }
        #[repr(C, align(4096))]
        struct AlignBlock([u8; ALIGN_SIZE]);

        let block_cnt = (n + ALIGN_SIZE - 1) / ALIGN_SIZE;
        let mut blocks: Vec<AlignBlock> = Vec::with_capacity(block_cnt);
        let ptr = blocks.as_mut_ptr();
        let cap_cnt = blocks.capacity();
        std::mem::forget(blocks);

        unsafe {
            Vec::from_raw_parts(
                ptr as *mut u8,
                0,
                cap_cnt * std::mem::size_of::<AlignBlock>(),
            )
        }
    }

    async fn write(&mut self, page: &[u8]) -> Result<u64> {
        let mut page_consumed = 0;
        while page_consumed < page.len() {
            let avaliable_buf = self.buffer.capacity() - self.buffer.len();
            if avaliable_buf > 0 {
                let fill_end = (page_consumed + avaliable_buf).min(page.len());
                self.buffer
                    .extend_from_slice(&page[page_consumed..fill_end]);
                page_consumed = fill_end;
            } else {
                self.flush().await?;
            }
        }
        let page_offset = self.next_page_offset;
        self.next_page_offset += page.len() as u64;
        Ok(page_offset)
    }

    async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if self.use_direct_io {
            self.actual_data_size += self.buffer.len();
            let align_len = Self::aligned_size(self.buffer.len());
            self.buffer.resize(align_len, 0);
        }
        self.file
            .write_all(&self.buffer)
            .await
            .expect("flush page file error");
        self.buffer.truncate(0);
        Ok(())
    }

    fn aligned_size(origin_size: usize) -> usize {
        ((origin_size + ALIGN_SIZE - 1) as u64 & ALIGN_MASK) as usize
    }

    async fn flush_and_sync(&mut self) -> Result<()> {
        self.flush().await?;
        if self.use_direct_io {
            self.file
                .set_len(self.actual_data_size as u64)
                .await
                .expect("set set file len fail");
        }
        // panic when sync fail, https://wiki.postgresql.org/wiki/Fsync_Errors
        self.file.sync_all().await.expect("sync file fail");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use photonio::io::ReadAtExt;

    use super::*;

    #[photonio::test]
    async fn test_buffered_writer() {
        let (use_direct, flags) = (true, 0x4000);
        let path1 = std::env::temp_dir().join("buf_test1");
        {
            let file1 = photonio::fs::OpenOptions::new()
                .write(true)
                .custom_flags(flags)
                .create(true)
                .truncate(true)
                .open(path1.to_owned())
                .await
                .expect("open file_id: {file_id}'s file fail");
            let mut bw1 = BufferedWriter::new(file1, 4096 + 1, use_direct);
            bw1.write(&[1].repeat(10)).await.unwrap(); // only fill buffer
            bw1.write(&[2].repeat(4096 * 2 + 1)).await.unwrap(); // trigger flush
            bw1.write(&[3].repeat(4096 * 2 + 1)).await.unwrap(); // trigger flush
            bw1.flush_and_sync().await.unwrap(); // flush again
        }
        {
            let file2 = photonio::fs::OpenOptions::new()
                .read(true)
                .open(path1)
                .await
                .expect("open file_id: {file_id}'s file fail");
            let mut buf = vec![0u8; 1];
            file2.read_exact_at(&mut buf, 4096 * 3).await.unwrap();
            assert_eq!(buf[0], 3)
        }
    }
}
