use std::{
    alloc::Layout,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use photonio::{
    fs::{File, Metadata},
    io::WriteExt,
};

use super::{types::split_page_addr, FileInfo, FileMeta};
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
    file_id: u32,
    writer: BufferedWriter,

    index: IndexBlockBuilder,
    meta: MetaBlockBuilder,

    last_add_page_id: u64,
    block_size: usize,
}

impl FileBuilder {
    /// Create new file builder for given writer.
    pub(crate) fn new(file_id: u32, file: File, use_direct: bool, block_size: usize) -> Self {
        let writer = BufferedWriter::new(file, IO_BUFFER_SIZE, use_direct, block_size);
        Self {
            file_id,
            writer,
            index: Default::default(),
            meta: Default::default(),
            last_add_page_id: 0,
            block_size,
        }
    }

    /// Add a new page to builder.
    pub(crate) async fn add_page(
        &mut self,
        page_id: u64,
        page_addr: u64,
        page_content: &[u8],
    ) -> Result<()> {
        if self.last_add_page_id >= page_id {
            return Err(Error::InvalidArgument);
        }
        self.last_add_page_id = page_id;

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
    pub(crate) async fn finish(&mut self) -> Result<FileInfo> {
        self.finish_meta_block().await?;
        let info = self.finish_file_footer().await?;
        self.writer.flush_and_sync().await?;
        Ok(info)
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

    async fn finish_file_footer(&mut self) -> Result<FileInfo> {
        let footer = {
            let (data_index, meta_index) = self.index.finish_index_block();
            Footer {
                magic: 142857,
                data_handle: BlockHandler {
                    offset: { self.writer.write(&data_index).await? },
                    length: data_index.len() as u64,
                },
                meta_handle: BlockHandler {
                    offset: { self.writer.write(&meta_index).await? },
                    length: meta_index.len() as u64,
                },
            }
        };

        let footer_dat = footer.encode();
        let foot_offset = self.writer.write(&footer_dat).await?;
        let file_size = foot_offset as usize + footer_dat.len();

        Ok(self.as_file_info(file_size, &footer))
    }

    fn as_file_info(&self, file_size: usize, footer: &Footer) -> FileInfo {
        let meta = {
            let (indexes, offsets) = self.index.index_block.as_meta_file_cached(footer);
            Arc::new(FileMeta::new(
                self.file_id,
                file_size as usize,
                indexes,
                offsets,
                self.block_size,
            ))
        };

        let active_pages = {
            let mut active_pages = roaring::RoaringBitmap::new();
            for (_page_id, page_addr) in &self.meta.page_table.0 {
                let (_, index) = split_page_addr(*page_addr);
                active_pages.insert(index);
            }
            active_pages
        };

        let active_size = meta.total_page_size();

        let file_id = meta.get_file_id();
        FileInfo::new(active_pages, active_size, file_id, file_id, meta)
    }
}

#[derive(Default)]
pub(crate) struct BlockHandler {
    pub(crate) offset: u64,
    pub(crate) length: u64,
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

pub(crate) struct Footer {
    magic: u64,
    pub(crate) data_handle: BlockHandler,
    pub(crate) meta_handle: BlockHandler,
}

impl Footer {
    #[inline]
    pub(crate) fn size() -> u32 {
        (core::mem::size_of::<u64>() * 5) as u32
    }

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(core::mem::size_of::<u64>() * 5);
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        self.data_handle.encode(&mut bytes);
        self.meta_handle.encode(&mut bytes);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::size() as usize {
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
pub(crate) struct PageTable(BTreeMap<u64, u64>);

impl PageTable {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.0.len() * core::mem::size_of::<u64>() * 2);
        for (page_addr, page_id) in &self.0 {
            bytes.extend_from_slice(&page_addr.to_le_bytes());
            bytes.extend_from_slice(&page_id.to_le_bytes())
        }
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
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

impl From<PageTable> for BTreeMap<u64, u64> {
    fn from(t: PageTable) -> Self {
        t.0
    }
}

#[derive(Default)]
pub(crate) struct DeletePages(BTreeSet<u64>);

impl DeletePages {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.0.len() * core::mem::size_of::<u64>());
        for delete_page_addr in &self.0 {
            bytes.extend_from_slice(&delete_page_addr.to_le_bytes());
        }
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
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

impl From<DeletePages> for Vec<u64> {
    fn from(d: DeletePages) -> Self {
        d.0.iter().cloned().collect::<Vec<u64>>()
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
pub(crate) struct IndexBlock {
    pub(crate) page_offsets: BTreeMap<u64, u64>,
    pub(crate) meta_page_table: Option<u64>,
    pub(crate) meta_delete_pages: Option<u64>,
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

    pub(crate) fn decode(data_index_bytes: &[u8], meta_index_bytes: &[u8]) -> Result<Self> {
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

    pub(crate) fn as_meta_file_cached(&self, footer: &Footer) -> (Vec<u64>, BTreeMap<u64, u64>) {
        let indexes = vec![
            self.meta_page_table.as_ref().unwrap().to_owned(),
            self.meta_delete_pages.as_ref().unwrap().to_owned(),
            footer.data_handle.offset, // meta block's end is index_block's start.
        ];
        (indexes, self.page_offsets.to_owned())
    }
}

struct BufferedWriter {
    file: File,

    use_direct: bool,

    next_page_offset: u64,
    actual_data_size: usize,

    align_size: usize,
    buffer: AlignBuffer,
    buf_pos: usize,
}

impl BufferedWriter {
    fn new(file: File, io_batch_size: usize, use_direct: bool, align_size: usize) -> Self {
        let buffer = AlignBuffer::new(io_batch_size, align_size);
        Self {
            file,
            use_direct,
            next_page_offset: 0,
            actual_data_size: 0,
            align_size,
            buffer,
            buf_pos: 0,
        }
    }

    async fn write(&mut self, page: &[u8]) -> Result<u64> {
        let mut page_consumed = 0;
        let buf_cap = self.buffer.len();
        while page_consumed < page.len() {
            if self.buf_pos < buf_cap {
                let free_buf = &mut self.buffer.as_bytes_mut()[self.buf_pos..buf_cap];
                let fill_end = (page_consumed + free_buf.len()).min(page.len());
                free_buf[..(fill_end - page_consumed)]
                    .copy_from_slice(&page[page_consumed..fill_end]);
                self.buf_pos += fill_end - page_consumed;
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
        if self.buf_pos == 0 {
            return Ok(());
        }
        if self.use_direct {
            self.actual_data_size += self.buf_pos;
            let align_len = ceil_to_block_hi_pos(self.buf_pos, self.align_size);
            self.buf_pos = align_len;
        }
        self.file
            .write_all(&self.buffer.as_bytes()[..self.buf_pos])
            .await
            .expect("flush page file error");
        self.buf_pos = 0;
        Ok(())
    }

    async fn flush_and_sync(&mut self) -> Result<()> {
        self.flush().await?;
        if self.use_direct {
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

const DEFAULT_BLOCK_SIZE: usize = 4096;

pub(crate) async fn logical_block_size(meta: &Metadata) -> usize {
    use std::os::unix::prelude::MetadataExt;
    // same as `major(3)` https://github.com/torvalds/linux/blob/5a18d07ce3006dbcb3c4cfc7bf1c094a5da19540/tools/include/nolibc/types.h#L191
    let major = (meta.dev() >> 8) & 0xfff;
    if let Ok(block_size_str) =
        std::fs::read_to_string(format!("/sys/dev/block/{major}:0/queue/logical_block_size"))
    {
        let block_size_str = block_size_str.trim();
        if let Ok(size) = block_size_str.parse::<usize>() {
            return size;
        }
    }
    DEFAULT_BLOCK_SIZE
}

pub(crate) struct AlignBuffer {
    data: std::ptr::NonNull<u8>,
    layout: Layout,
    size: usize,
}

impl AlignBuffer {
    pub(crate) fn new(n: usize, align: usize) -> Self {
        assert!(n > 0);
        let size = ceil_to_block_hi_pos(n, align);
        let layout = Layout::from_size_align(size, align).expect("Invalid layout");
        let data = unsafe {
            // Safety: it is guaranteed that layout size > 0.
            std::ptr::NonNull::new(std::alloc::alloc(layout)).expect("The memory is exhausted")
        };
        Self { data, layout, size }
    }

    #[inline]
    fn len(&self) -> usize {
        self.size
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.size) }
    }

    pub(crate) fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.size) }
    }
}

impl Drop for AlignBuffer {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

/// # Safety
///
/// [`AlignBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Send for AlignBuffer {}

/// # Safety
///
/// [`AlignBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Sync for AlignBuffer {}

#[inline]
pub(crate) fn floor_to_block_lo_pos(pos: usize, align: usize) -> usize {
    pos - (pos & (align - 1))
}

#[inline]
pub(crate) fn ceil_to_block_hi_pos(pos: usize, align: usize) -> usize {
    ((pos + align - 1) / align) * align
}

#[inline]
pub(crate) fn is_block_algined_pos(pos: usize, align: usize) -> bool {
    (pos & (align - 1)) == 0
}

#[inline]
pub(crate) fn is_block_aligned_ptr(p: *const u8, align: usize) -> bool {
    p.is_aligned_to(align)
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
            let mut bw1 = BufferedWriter::new(file1, 4096 + 1, use_direct, 512);
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
