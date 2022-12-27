use std::{alloc::Layout, collections::BTreeMap, marker::PhantomData, sync::Arc};

use super::{
    checksum,
    compression::{compress_max_len, compress_page, Compression},
    ChecksumType, PageGroupMeta,
};
use crate::{
    env::{Directory, Env, SequentialWriter, SequentialWriterExt},
    page::PageInfo,
    page_store::{Error, Result},
};

pub(crate) struct CommonFileBuilder {
    group_id: u32,
    compression: Compression,
    checksum: ChecksumType,

    index: IndexBlockBuilder,
    page_table: PageTable,
}

impl CommonFileBuilder {
    pub(super) fn new(group_id: u32, compression: Compression, checksum: ChecksumType) -> Self {
        CommonFileBuilder {
            group_id,
            compression,
            checksum,
            index: IndexBlockBuilder::default(),
            page_table: PageTable::default(),
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
        self.page_table.0.insert(page_addr, page_id);
        Ok(())
    }

    pub(super) async fn finish_meta_block<'a, E: Env>(
        &mut self,
        writer: &mut BufferedWriter<'a, E>,
    ) -> Result<()> {
        let page_tables = self.page_table.encode();
        let file_offset = writer.write(&page_tables).await?;
        self.index.add_page_table_meta_block(file_offset);
        Ok(())
    }

    pub(super) async fn finish_index_block<'a, E: Env>(
        &mut self,
        writer: &mut BufferedWriter<'a, E>,
    ) -> Result<(
        BlockHandle, /* data index */
        BlockHandle, /* meta index */
    )> {
        let (data_index, meta_index) = self.index.finish_index_block();
        let data_offset = writer.write(&data_index).await?;
        let meta_offset = writer.write(&meta_index).await?;
        let data_handle = BlockHandle {
            offset: data_offset,
            length: data_index.len() as u64,
        };
        let meta_handle = BlockHandle {
            offset: meta_offset,
            length: meta_index.len() as u64,
        };
        Ok((data_handle, meta_handle))
    }

    pub(crate) fn as_page_group_meta(
        &self,
        file_id: u32,
        base_offset: u64,
        data_handle: BlockHandle,
    ) -> Arc<PageGroupMeta> {
        let (indexes, offsets) = self.index.index_block.as_meta_file_cached(data_handle);
        Arc::new(PageGroupMeta::new(
            self.group_id,
            file_id,
            base_offset,
            indexes,
            offsets,
        ))
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub(crate) struct BlockHandle {
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

impl BlockHandle {
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

        if meta_index_bytes.len() != core::mem::size_of::<u64>() {
            return Err(Error::Corrupted);
        }
        let meta_page_table = Some(u64::from_le_bytes(
            meta_index_bytes[0..core::mem::size_of::<u64>()]
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
        })
    }

    pub(crate) fn as_meta_file_cached(
        &self,
        data_handle: BlockHandle,
    ) -> (Vec<u64>, BTreeMap<u64, (u64, PageInfo)>) {
        let indexes = vec![
            self.meta_page_table.as_ref().unwrap().to_owned(),
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
