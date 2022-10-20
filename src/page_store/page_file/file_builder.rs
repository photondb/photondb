use std::collections::{BTreeMap, BTreeSet, HashMap};

use photonio::{fs::File, io::WriteExt};

use crate::page_store::{Error, Result};

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
/// footer = {magic_number} {data block index} {meta block index}
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
            self.index.add_page_table_meta_block(file_offset)
        }
        {
            let delete_pages = self.meta.finish_delete_pages_block();
            let file_offset = self.writer.write(&delete_pages).await?;
            self.index.add_delete_pages_meta_block(file_offset)
        };
        Ok(())
    }

    async fn finish_file_footer(&mut self) -> Result<()> {
        let footer = {
            let mut f = Footer::default();
            f.magic = 142857;
            let (data_index, meta_index) = self.index.finish_index_block();
            f.data_handle.offset = self.writer.write(&data_index).await?;
            f.data_handle.length = data_index.len() as u64;
            f.meta_handle.offset = self.writer.write(&meta_index).await?;
            f.meta_handle.length = meta_index.len() as u64;
            f
        };

        let footer_dat = footer.encode();
        self.writer.write(&footer_dat).await?;

        Ok(())
    }
}

#[derive(Default)]
struct BlockHandler {
    offset: u64,
    length: u64,
}

impl BlockHandler {
    fn encode(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.offset.to_le_bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != core::mem::size_of::<u64>() * 2 {
            return Err(Error::Corrupted);
        }
        let offset = u64::from_le_bytes(
            bytes[0..core::mem::size_of::<u64>()]
                .try_into()
                .map_err(|_| Error::Corrupted)?,
        );
        let length = u64::from_le_bytes(
            bytes[core::mem::size_of::<u64>()..core::mem::size_of::<u64>() * 2]
                .try_into()
                .map_err(|_| Error::Corrupted)?,
        );
        Ok(Self { offset, length })
    }
}

#[derive(Default)]
struct Footer {
    magic: u64,
    data_handle: BlockHandler,
    meta_handle: BlockHandler,
}

impl Footer {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(core::mem::size_of::<u64>() * 5);
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        self.data_handle.encode(&mut bytes);
        self.meta_handle.encode(&mut bytes);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != core::mem::size_of::<u64>() * 5 {
            return Err(Error::Corrupted);
        }
        let mut idx = 0;
        let magic = u64::from_le_bytes(
            bytes[idx..idx + core::mem::size_of::<u64>()]
                .try_into()
                .map_err(|_| Error::Corrupted)?,
        );
        idx += core::mem::size_of::<u64>();
        let data_handle = BlockHandler::decode(&bytes[idx..idx + core::mem::size_of::<u64>() * 2])?;
        idx += core::mem::size_of::<u64>() * 2;
        let meta_handle = BlockHandler::decode(&bytes[idx..idx + core::mem::size_of::<u64>() * 2])?;
        Ok(Self {
            magic,
            data_handle,
            meta_handle,
        })
    }
}

#[derive(Default)]
struct MetaBlockBuilder {
    delete_page_addrs: DeletePages,
    page_table: PageTable,
}

impl MetaBlockBuilder {
    pub(crate) fn add_page(&mut self, page_id: u64, page_addr: u64) {
        self.page_table.0.insert(page_id, page_addr);
    }

    pub(crate) fn delete_pages(&mut self, page_addrs: &[u64]) {
        for page_addr in page_addrs {
            self.delete_page_addrs.0.insert(*page_addr);
        }
    }

    pub(crate) fn finish_page_table_block(&self) -> Vec<u8> {
        self.page_table.encode()
    }

    pub(crate) fn finish_delete_pages_block(&self) -> Vec<u8> {
        self.delete_page_addrs.encode()
    }
}

#[derive(Default)]
struct PageTable(BTreeMap<u64, u64>);

impl PageTable {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.0.len() * core::mem::size_of::<u64>() * 2);
        for (page_addr, page_id) in &self.0 {
            bytes.extend_from_slice(&page_addr.to_le_bytes());
            bytes.extend_from_slice(&page_id.to_le_bytes())
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut table = BTreeMap::new();
        let mut idx = 0;
        while idx < bytes.len() {
            let end = idx + core::mem::size_of::<u64>() * 2;
            if end > bytes.len() {
                return Err(Error::Corrupted);
            }
            let page_addr = u64::from_le_bytes(
                bytes[idx..idx + core::mem::size_of::<u64>()]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            );
            let page_id = u64::from_le_bytes(
                bytes[idx + core::mem::size_of::<u64>()..idx + core::mem::size_of::<u64>() * 2]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            );
            table.insert(page_addr, page_id);
            idx = end;
        }
        Ok(PageTable(table))
    }
}

#[derive(Default)]
struct DeletePages(BTreeSet<u64>);

impl DeletePages {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.0.len() * core::mem::size_of::<u64>());
        for delete_page_addr in &self.0 {
            bytes.extend_from_slice(&delete_page_addr.to_le_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut pages = BTreeSet::new();
        let mut idx = 0;
        while idx < bytes.len() {
            let end = idx + core::mem::size_of::<u64>();
            if end > bytes.len() {
                return Err(Error::Corrupted);
            }
            let del_page =
                u64::from_le_bytes(bytes[idx..end].try_into().map_err(|_| Error::Corrupted)?);
            pages.insert(del_page);
            idx = end;
        }
        Ok(DeletePages(pages))
    }
}

#[derive(Default)]
struct IndexBlockBuilder {
    index_block: IndexBlock,
}

impl IndexBlockBuilder {
    #[inline]
    pub(crate) fn add_data_block(&mut self, page_addr: u64, file_offset: u64) {
        self.index_block.page_offsets.insert(page_addr, file_offset);
    }

    #[inline]
    pub(crate) fn add_page_table_meta_block(&mut self, file_offset: u64) {
        self.index_block.meta_page_table = Some(file_offset);
    }

    #[inline]
    pub(crate) fn add_delete_pages_meta_block(&mut self, file_offset: u64) {
        self.index_block.meta_delete_pages = Some(file_offset);
    }

    pub(crate) fn finish_index_block(
        &self,
    ) -> (Vec<u8> /* data index */, Vec<u8> /* meta index */) {
        (
            self.index_block.encode_data_block_index(),
            self.index_block.encode_meta_block_index(),
        )
    }
}

#[derive(Default)]
struct IndexBlock {
    page_offsets: BTreeMap<u64, u64>,
    meta_page_table: Option<u64>,
    meta_delete_pages: Option<u64>,
}

impl IndexBlock {
    fn encode_data_block_index(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(self.page_offsets.len() * core::mem::size_of::<u64>() * 2);
        for (page_addr, offset) in &self.page_offsets {
            bytes.extend_from_slice(&page_addr.to_le_bytes());
            bytes.extend_from_slice(&offset.to_le_bytes())
        }
        bytes
    }

    fn encode_meta_block_index(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(core::mem::size_of::<u64>() * 2);
        bytes.extend_from_slice(&self.meta_page_table.as_ref().unwrap().to_le_bytes());
        bytes.extend_from_slice(&self.meta_delete_pages.as_ref().unwrap().to_le_bytes());
        bytes
    }

    fn decode(data_index_bytes: &[u8], meta_index_bytes: &[u8]) -> Result<Self> {
        let mut page_offsets = BTreeMap::new();
        let mut idx = 0;
        while idx < data_index_bytes.len() {
            let end = idx + core::mem::size_of::<u64>() * 2;
            if end > data_index_bytes.len() {
                return Err(Error::Corrupted);
            }
            let page_addr = u64::from_le_bytes(
                data_index_bytes[idx..idx + core::mem::size_of::<u64>()]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            );
            let page_id = u64::from_le_bytes(
                data_index_bytes
                    [idx + core::mem::size_of::<u64>()..idx + core::mem::size_of::<u64>() * 2]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            );
            page_offsets.insert(page_addr, page_id);
            idx = end;
        }

        if meta_index_bytes.len() != core::mem::size_of::<u64>() * 2 {
            return Err(Error::Corrupted);
        }
        let meta_page_table = Some(u64::from_le_bytes(
            meta_index_bytes[0..core::mem::size_of::<u64>()]
                .try_into()
                .map_err(|_| Error::Corrupted)?,
        ));
        let meta_delete_pages = Some(u64::from_le_bytes(
            meta_index_bytes[core::mem::size_of::<u64>()..core::mem::size_of::<u64>() * 2]
                .try_into()
                .map_err(|_| Error::Corrupted)?,
        ));
        Ok(Self {
            page_offsets,
            meta_page_table,
            meta_delete_pages,
        })
    }
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

    use super::*;

    #[photonio::test]
    async fn test_buffered_writer() {
        use std::os::unix::prelude::OpenOptionsExt;

        use photonio::io::ReadAtExt;

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
            assert_eq!(buf[0], 3);
            let length = file2.metadata().await.unwrap().len();
            assert_eq!(length, 10 + (4096 * 2 + 1) * 2)
        }
    }
}
