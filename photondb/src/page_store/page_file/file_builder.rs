use std::{
    alloc::Layout,
    collections::{BTreeMap, BTreeSet, HashSet},
    marker::PhantomData,
    sync::Arc,
};

use super::{
    checksum,
    compression::{compress_max_len, compress_page, Compression},
    constant::*,
    types::split_page_addr,
    ChecksumType, FileInfo, FileMeta,
};
use crate::{
    env::{Directory, Env, SequentialWriter, SequentialWriterExt},
    page::PageInfo,
    page_store::{Error, Result},
};

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

#[allow(unused)]
pub(crate) struct FileBuilder<'a, E: Env> {
    writer: BufferedWriter<'a, E>,
    inner: CommonFileBuilder,
}

impl<'a, E: Env> FileBuilder<'a, E> {
    /// Create new file builder for given writer.
    pub(crate) fn new(
        file_id: u32,
        base_dir: &'a E::Directory,
        file: E::SequentialWriter,
        use_direct: bool,
        block_size: usize,
        comperssion: Compression,
        checksum: ChecksumType,
    ) -> Self {
        let writer = BufferedWriter::new(file, IO_BUFFER_SIZE, use_direct, block_size, base_dir);
        Self {
            writer,
            inner: CommonFileBuilder::new(file_id, block_size, comperssion, checksum),
        }
    }

    /// Add a new page to builder.

    #[allow(unused)]
    pub(crate) async fn add_page(
        &mut self,
        page_id: u64,
        page_addr: u64,
        page_info: PageInfo,
        page_content: &[u8],
    ) -> Result<()> {
        self.inner
            .add_page(
                &mut self.writer,
                page_id,
                page_addr,
                page_info,
                page_content,
            )
            .await
    }

    /// Add delete page to builder.
    #[allow(unused)]
    pub(crate) fn add_delete_pages(&mut self, page_addrs: &[u64]) {
        self.inner.add_delete_pages(page_addrs)
    }

    // Finish build page file.
    #[allow(unused)]
    pub(crate) async fn finish(&mut self) -> Result<FileInfo> {
        self.inner.finish_meta_block(&mut self.writer).await?;
        let info = self.finish_file_footer().await?;
        self.writer.flush_and_sync().await?;
        Ok(info)
    }
}

impl<'a, E: Env> FileBuilder<'a, E> {
    async fn finish_file_footer(&mut self) -> Result<FileInfo> {
        let (data_handle, meta_handle) = self.inner.finish_index_block(&mut self.writer).await?;
        let footer = Footer {
            magic: PAGE_FILE_MAGIC,
            data_handle,
            meta_handle,
            compression: self.inner.compression,
            checksum_type: self.inner.checksum,
        };

        let footer_dat = footer.encode();
        let foot_offset = self.writer.write(&footer_dat).await?;
        let file_size = foot_offset as usize + footer_dat.len();

        let meta = self.inner.as_page_file_meta(file_size, footer.data_handle);
        let staled_pages = meta.dealloc_pages_bitmap();
        let active_size = meta.total_page_size();
        Ok(FileInfo::new(
            staled_pages,
            active_size,
            self.inner.file_id,
            self.inner.file_id,
            self.inner.get_referenced_files(),
            meta,
        ))
    }
}

pub(crate) struct CommonFileBuilder {
    file_id: u32,
    block_size: usize,
    compression: Compression,
    checksum: ChecksumType,

    index: IndexBlockBuilder,
    meta: MetaBlockBuilder,
}

impl CommonFileBuilder {
    pub(super) fn new(
        file_id: u32,
        block_size: usize,
        compression: Compression,
        checksum: ChecksumType,
    ) -> Self {
        CommonFileBuilder {
            file_id,
            block_size,
            compression,
            checksum,
            index: IndexBlockBuilder::default(),
            meta: MetaBlockBuilder::default(),
        }
    }

    /// Add a new page to builder.
    pub(super) async fn add_page<'a, E: Env>(
        &mut self,
        writer: &mut BufferedWriter<'a, E>,
        page_id: u64,
        page_addr: u64,
        page_info: PageInfo,
        page_content: &[u8],
    ) -> Result<()> {
        let mut tmp_buf = vec![0u8; compress_max_len(self.compression, page_content)]; // TODO: pool this.
        let page_content = compress_page(self.compression, page_content, &mut tmp_buf)?;
        let checksum = checksum::checksum(self.checksum, page_content);
        let file_offset = writer.write_with_checksum(page_content, checksum).await?;
        self.index.add_data_block(page_addr, file_offset, page_info);
        self.meta.add_page(page_addr, page_id);
        Ok(())
    }

    /// Add delete page to builder.
    pub(super) fn add_delete_pages(&mut self, page_addrs: &[u64]) {
        self.meta.delete_pages(page_addrs)
    }

    pub(super) async fn finish_meta_block<'a, E: Env>(
        &mut self,
        writer: &mut BufferedWriter<'a, E>,
    ) -> Result<()> {
        {
            let page_tables = self.meta.finish_page_table_block();
            let file_offset = writer.write(&page_tables).await?;
            self.index.add_page_table_meta_block(file_offset)
        }
        {
            let delete_pages = self.meta.finish_delete_pages_block();
            let file_offset = writer.write(&delete_pages).await?;
            self.index.add_delete_pages_meta_block(file_offset)
        };
        Ok(())
    }

    pub(super) async fn finish_index_block<'a, E: Env>(
        &mut self,
        writer: &mut BufferedWriter<'a, E>,
    ) -> Result<(
        BlockHandler, /* data index */
        BlockHandler, /* meta index */
    )> {
        let (data_index, meta_index) = self.index.finish_index_block();
        let data_offset = writer.write(&data_index).await?;
        let meta_offset = writer.write(&meta_index).await?;
        let data_handle = BlockHandler {
            offset: data_offset,
            length: data_index.len() as u64,
        };
        let meta_handle = BlockHandler {
            offset: meta_offset,
            length: meta_index.len() as u64,
        };
        Ok((data_handle, meta_handle))
    }

    pub(crate) fn get_referenced_files(&self) -> HashSet<u32> {
        let mut files = HashSet::new();
        for page_addr in self.meta.get_deleted_pages() {
            let (file_id, _) = split_page_addr(page_addr);
            files.insert(file_id);
        }
        files
    }

    pub(crate) fn as_partial_file_meta(
        &self,
        map_file_id: u32,
        base_offset: u64,
        data_handle: BlockHandler,
    ) -> Arc<FileMeta> {
        let (indexes, offsets) = self.index.index_block.as_meta_file_cached(data_handle);
        Arc::new(FileMeta::new_partial(
            self.file_id,
            map_file_id,
            base_offset,
            self.block_size,
            indexes,
            offsets,
            self.compression,
            self.checksum,
        ))
    }

    #[allow(unused)]
    pub(super) fn as_page_file_meta(
        &self,
        file_size: usize,
        data_handle: BlockHandler,
    ) -> Arc<FileMeta> {
        let (indexes, offsets) = self.index.index_block.as_meta_file_cached(data_handle);
        Arc::new(FileMeta::new(
            self.file_id,
            file_size as usize,
            self.block_size,
            indexes,
            offsets,
            self.compression,
            self.checksum,
        ))
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub(crate) struct BlockHandler {
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

impl BlockHandler {
    pub(super) const fn encoded_size() -> usize {
        core::mem::size_of::<u64>() * 2
    }

    pub(super) fn encode(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.offset.to_le_bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
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

#[allow(unused)]
pub(crate) struct Footer {
    magic: u64,
    pub(crate) data_handle: BlockHandler,
    pub(crate) meta_handle: BlockHandler,
    pub(crate) compression: Compression,
    pub(crate) checksum_type: ChecksumType,
}

impl Footer {
    #[inline]
    pub(crate) fn size() -> u32 {
        (core::mem::size_of::<u64>() * 5 + 1 + 1) as u32
    }

    #[allow(unused)]
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(core::mem::size_of::<u64>() * 5);
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        self.data_handle.encode(&mut bytes);
        self.meta_handle.encode(&mut bytes);
        bytes.push(self.compression.bits());
        bytes.push(self.checksum_type.bits());
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
        idx += core::mem::size_of::<u64>() * 2;
        let compression = Compression::from_bits(bytes[idx]).ok_or(Error::Corrupted)?;
        idx += 1;
        let checksum_type = ChecksumType::from_bits(bytes[idx]).ok_or(Error::Corrupted)?;
        Ok(Self {
            magic,
            data_handle,
            meta_handle,
            compression,
            checksum_type,
        })
    }
}

#[derive(Default)]
struct MetaBlockBuilder {
    delete_page_addrs: DeletePages,
    page_table: PageTable,
}

impl MetaBlockBuilder {
    pub(crate) fn add_page(&mut self, page_addr: u64, page_id: u64) {
        self.page_table.0.insert(page_addr, page_id);
    }

    #[allow(unused)]
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

    #[inline]
    pub(crate) fn get_deleted_pages(&self) -> HashSet<u64> {
        self.delete_page_addrs.0.iter().cloned().collect()
    }
}

#[derive(Default)]
pub(crate) struct PageTable(BTreeMap<u64 /* page addr */, u64 /* page id */>);

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
    pub(crate) fn add_data_block(&mut self, page_addr: u64, file_offset: u64, page_info: PageInfo) {
        self.index_block
            .page_offsets
            .insert(page_addr, (file_offset, page_info));
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
    pub(crate) page_offsets: BTreeMap<u64, (u64, PageInfo)>,
    pub(crate) meta_page_table: Option<u64>,
    pub(crate) meta_delete_pages: Option<u64>,
}

impl IndexBlock {
    fn encode_data_block_index(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(self.page_offsets.len() * core::mem::size_of::<u64>() * 4);
        for (page_addr, (offset, page_info)) in &self.page_offsets {
            bytes.extend_from_slice(&page_addr.to_le_bytes());
            bytes.extend_from_slice(&offset.to_le_bytes());
            let (meta, next) = page_info.value();
            bytes.extend_from_slice(&meta.to_le_bytes());
            bytes.extend_from_slice(&next.to_le_bytes());
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
        let mut last_offset = None;
        while idx < data_index_bytes.len() {
            let end = idx + core::mem::size_of::<u64>() * 4;
            if end > data_index_bytes.len() {
                return Err(Error::Corrupted);
            }

            let end = idx + core::mem::size_of::<u64>();
            let page_addr = u64::from_le_bytes(
                data_index_bytes[idx..end]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            );

            idx = end;
            let end = idx + core::mem::size_of::<u64>();
            let offset = u64::from_le_bytes(
                data_index_bytes[idx..end]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            );

            idx = end;
            let end = idx + core::mem::size_of::<u64>();
            let meta = u64::from_le_bytes(
                data_index_bytes[idx..end]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            );

            idx = end;
            let end = idx + core::mem::size_of::<u64>();
            let next = u64::from_le_bytes(
                data_index_bytes[idx..end]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            );
            if let Some((addr, last_offset, meta, next)) = last_offset {
                let size = (offset - last_offset) as usize;
                page_offsets.insert(addr, (last_offset, PageInfo::from_raw(meta, next, size)));
            }
            last_offset = Some((page_addr, offset, meta, next));
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
        if let Some((addr, offset, meta, next)) = last_offset {
            let size = (meta_page_table.unwrap() - offset) as usize;
            page_offsets.insert(addr, (offset, PageInfo::from_raw(meta, next, size)));
        }
        Ok(Self {
            page_offsets,
            meta_page_table,
            meta_delete_pages,
        })
    }

    pub(crate) fn as_meta_file_cached(
        &self,
        data_handle: BlockHandler,
    ) -> (Vec<u64>, BTreeMap<u64, (u64, PageInfo)>) {
        let indexes = vec![
            self.meta_page_table.as_ref().unwrap().to_owned(),
            self.meta_delete_pages.as_ref().unwrap().to_owned(),
            data_handle.offset, // meta block's end is index_block's start.
        ];
        (indexes, self.page_offsets.to_owned())
    }
}

pub(super) struct BufferedWriter<'a, E: Env> {
    file: E::SequentialWriter,
    base_dir: &'a E::Directory,

    use_direct: bool,

    next_page_offset: u64,
    actual_data_size: usize,

    align_size: usize,
    buffer: AlignBuffer,
    buf_pos: usize,
    _mark: PhantomData<E>,
}

impl<'a, E: Env> BufferedWriter<'a, E> {
    pub(super) fn new(
        file: E::SequentialWriter,
        io_batch_size: usize,
        use_direct: bool,
        align_size: usize,
        base_dir: &'a E::Directory,
    ) -> Self {
        let buffer = AlignBuffer::new(io_batch_size, align_size);
        Self {
            file,
            base_dir,
            use_direct,
            next_page_offset: 0,
            actual_data_size: 0,
            align_size,
            buffer,
            buf_pos: 0,
            _mark: PhantomData,
        }
    }

    pub(super) async fn write(&mut self, page: &[u8]) -> Result<u64> {
        self.write_with_checksum(page, None).await
    }

    pub(super) async fn write_with_checksum(
        &mut self,
        page: &[u8],
        checksum: Option<u32>,
    ) -> Result<u64> {
        self.fill_buf(page).await?;

        if let Some(checksum) = checksum {
            let checksum_bytes = checksum.to_le_bytes();
            self.fill_buf(&checksum_bytes).await?;
        }

        let page_offset = self.next_page_offset;
        self.next_page_offset += if checksum.is_none() {
            page.len() as u64
        } else {
            const CHECKSUM_LEN: usize = std::mem::size_of::<u32>();
            page.len() as u64 + CHECKSUM_LEN as u64
        };
        Ok(page_offset)
    }

    async fn fill_buf(&mut self, data: &[u8]) -> Result<()> {
        let buf_cap = self.buffer.len();
        let mut consumed = 0;
        while consumed < data.len() {
            if self.buf_pos < buf_cap {
                let free_buf = &mut self.buffer.as_bytes_mut()[self.buf_pos..buf_cap];
                let fill_end = (consumed + free_buf.len()).min(data.len());
                free_buf[..(fill_end - consumed)].copy_from_slice(&data[consumed..fill_end]);
                self.buf_pos += fill_end - consumed;
                consumed = fill_end;
            } else {
                self.flush().await?;
            }
        }
        Ok(())
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

    pub(crate) async fn flush_and_sync(&mut self) -> Result<()> {
        self.flush().await?;
        if self.use_direct {
            self.file
                .truncate(self.actual_data_size as u64)
                .await
                .expect("set set file len fail");
        }
        // panic when sync fail, https://wiki.postgresql.org/wiki/Fsync_Errors
        self.file.sync_all().await.expect("sync file fail");
        self.base_dir.sync_all().await.expect("sync base dir fail");
        Ok(())
    }

    #[inline]
    pub(super) fn next_offset(&self) -> u64 {
        self.next_page_offset
    }
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
pub(crate) fn is_block_aligned_pos(pos: usize, align: usize) -> bool {
    (pos & (align - 1)) == 0
}

#[inline]
pub(crate) fn is_block_aligned_ptr(p: *const u8, align: usize) -> bool {
    p.is_aligned_to(align)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::env::Env;

    #[cfg(unix)]
    #[photonio::test]
    async fn test_buffered_writer() {
        use tempdir::TempDir;

        use crate::env::PositionalReaderExt;

        let env = crate::env::Photon;

        let use_direct = true;
        let base_dir = TempDir::new("buffer_writer").unwrap();
        let path1 = base_dir.path().join("buf_test1");
        let base = env.open_dir(base_dir.path()).await.unwrap();
        {
            let file1 = env
                .open_sequential_writer(path1.to_owned())
                .await
                .expect("open file_id: {file_id}'s file fail");
            let mut bw1 =
                BufferedWriter::<crate::env::Photon>::new(file1, 4096 + 1, use_direct, 512, &base);
            bw1.write(&[1].repeat(10)).await.unwrap(); // only fill buffer
            bw1.write(&[2].repeat(4096 * 2 + 1)).await.unwrap(); // trigger flush
            bw1.write(&[3].repeat(4096 * 2 + 1)).await.unwrap(); // trigger flush
            bw1.flush_and_sync().await.unwrap(); // flush again
        }
        {
            let file2 = env
                .open_positional_reader(path1.to_owned())
                .await
                .expect("open file_id: {file_id}'s file fail");
            let mut buf = vec![0u8; 1];
            file2.read_exact_at(&mut buf, 4096 * 3).await.unwrap();
            assert_eq!(buf[0], 3);
            let length = env.metadata(path1).await.unwrap().len;
            assert_eq!(length, 10 + (4096 * 2 + 1) * 2)
        }
    }
}
