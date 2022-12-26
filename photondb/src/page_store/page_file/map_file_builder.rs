#![allow(unused)]
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use super::{
    compression::Compression,
    constant::*,
    types::{split_page_addr, MapFileMeta},
    BlockHandler, BufferedWriter, ChecksumType, CommonFileBuilder, FileInfo, MapFileInfo,
};
use crate::{
    env::Env,
    page::PageInfo,
    page_store::{Error, Result},
};

/// Builder for map file.
///
/// File format:
///
/// File = [{page file}] {page block index} {dealloc pages block} {footer}
/// page file = {data blocks} {meta blocks} {index blocks}
/// data blocks = [{data block}] --- one block per tree page
/// meta blocks = {page table block}
/// page table block = [(page_id, page_addr[low 32bit])]
/// index_blocks = {data block index} {meta block index}
/// data block index = {page_addr[low 32bit], file_offset}
/// meta block index = {file_offset}
/// page block index = [(page_id, {data block index}, {meta block index})]
/// dealloc pages block = [dealloc_page_addr]
/// footer = {magic_number} { page block index}
pub(crate) struct MapFileBuilder<'a, E: Env> {
    file_id: u32,
    writer: BufferedWriter<'a, E>,
    dealloc_pages: BTreeSet<u64>,
    page_index: PageIndexBuilder,
    file_infos: HashMap<u32, FileInfo>,
    block_size: usize,
    file_offset: usize,
    compression: Compression,
    checksum: ChecksumType,
}

/// File builder for partial of map file.
pub(crate) struct PartialFileBuilder<'a, E: Env> {
    file_id: u32,
    base_offset: u64,
    builder: MapFileBuilder<'a, E>,
    inner: CommonFileBuilder,
}

/// Builder for records page indexes.
#[derive(Default)]
pub(crate) struct PageIndexBuilder {
    pages: Vec<PageIndex>,
}

/// A handler for partial page file.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PageIndex {
    pub(super) file_id: u32,
    pub(super) data_handle: BlockHandler,
    pub(super) meta_handle: BlockHandler,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Footer {
    pub(super) magic: u64,
    pub(super) page_index_handle: BlockHandler,
    pub(super) dealloc_pages_handle: BlockHandler,
    pub(super) compression: Compression,
    pub(super) checksum_type: ChecksumType,
}

impl<'a, E: Env> MapFileBuilder<'a, E> {
    pub(crate) fn new(
        file_id: u32,
        base_dir: &'a E::Directory,
        file: E::SequentialWriter,
        use_direct: bool,
        block_size: usize,
        compression: Compression,
        checksum: ChecksumType,
    ) -> Self {
        let writer = BufferedWriter::new(file, IO_BUFFER_SIZE, use_direct, block_size, base_dir);
        Self {
            file_id,
            writer,
            dealloc_pages: BTreeSet::default(),
            page_index: PageIndexBuilder::default(),
            file_infos: HashMap::default(),
            file_offset: 0,
            block_size,
            compression,
            checksum,
        }
    }

    pub(crate) fn add_file(self, file_id: u32) -> PartialFileBuilder<'a, E> {
        let block_size = self.block_size;
        let compression = self.compression;
        let checksum_type = self.checksum;
        let base_offset = self.writer.next_offset();
        PartialFileBuilder {
            file_id,
            base_offset,
            builder: self,
            inner: CommonFileBuilder::new(file_id, block_size, compression, checksum_type),
        }
    }

    pub(crate) async fn finish(
        mut self,
        up2: u32,
    ) -> Result<(HashMap<u32, FileInfo>, MapFileInfo)> {
        let file_size = self.finish_tail_blocks().await?;
        self.writer.flush_and_sync().await?;
        let page_files = self
            .file_infos
            .iter()
            .map(|(&id, info)| (id, info.meta()))
            .collect::<HashMap<_, _>>();
        let file_meta = Arc::new(MapFileMeta::new(
            self.file_id,
            file_size,
            DEFAULT_BLOCK_SIZE,
            page_files,
        ));
        let file_info = MapFileInfo::new(up2, up2, self.get_referenced_files(), file_meta);
        Ok((self.file_infos, file_info))
    }

    async fn finish_tail_blocks(&mut self) -> Result<usize> {
        let page_index_handle = self.finish_page_index_block().await?;
        let dealloc_pages_handle = self.finish_dealloc_pages_block().await?;
        let footer = Footer {
            magic: MAP_FILE_MAGIC,
            page_index_handle,
            dealloc_pages_handle,
            compression: self.compression,
            checksum_type: self.checksum,
        };
        let payload = footer.encode();
        let foot_offset = self.writer.write(&payload).await?;
        Ok(foot_offset as usize + payload.len())
    }

    async fn finish_page_index_block(&mut self) -> Result<BlockHandler> {
        let page_index_block = self.page_index.finish();
        let offset = self.writer.write(&page_index_block).await?;
        let length = page_index_block.len() as u64;
        Ok(BlockHandler { offset, length })
    }

    async fn finish_dealloc_pages_block(&mut self) -> Result<BlockHandler> {
        let estimated_size = core::mem::size_of::<u64>() * self.dealloc_pages.len();
        let mut buf = Vec::with_capacity(estimated_size);
        for addr in &self.dealloc_pages {
            buf.extend_from_slice(&addr.to_le_bytes());
        }
        let offset = self.writer.write(&buf).await?;
        let length = buf.len() as u64;
        Ok(BlockHandler { offset, length })
    }

    fn get_referenced_files(&self) -> HashSet<u32> {
        let mut files = HashSet::new();
        for page_addr in &self.dealloc_pages {
            let (file_id, _) = split_page_addr(*page_addr);
            files.insert(file_id);
        }
        files
    }
}

impl<'a, E: Env> PartialFileBuilder<'a, E> {
    /// Add a new page to builder.
    pub(crate) async fn add_page(
        &mut self,
        page_id: u64,
        page_addr: u64,
        page_info: PageInfo,
        page_content: &[u8],
    ) -> Result<()> {
        self.inner
            .add_page(
                &mut self.builder.writer,
                page_id,
                page_addr,
                page_info,
                page_content,
            )
            .await
    }

    /// Add some dealloc pages to builder.
    pub(crate) fn add_dealloc_pages(&mut self, dealloc_pages: &[u64]) {
        self.builder.dealloc_pages.extend(dealloc_pages);
    }

    pub(crate) async fn finish(mut self) -> Result<MapFileBuilder<'a, E>> {
        self.inner
            .finish_meta_block(&mut self.builder.writer)
            .await?;
        let (data, meta) = self
            .inner
            .finish_index_block(&mut self.builder.writer)
            .await?;
        self.builder
            .page_index
            .add_page_file(self.file_id, data, meta);

        let file_meta =
            self.inner
                .as_partial_file_meta(self.builder.file_id, self.base_offset, data);
        let staled_pages = file_meta.dealloc_pages_bitmap();
        let active_size = file_meta.total_page_size();
        self.builder.file_offset = self.builder.writer.next_offset() as usize;
        let file_info = FileInfo::new(
            staled_pages,
            active_size,
            self.file_id,
            self.file_id,
            self.inner.get_referenced_files(),
            file_meta,
        );
        self.builder.file_infos.insert(self.file_id, file_info);
        Ok(self.builder)
    }
}

impl PageIndexBuilder {
    fn add_page_file(
        &mut self,
        file_id: u32,
        data_handler: BlockHandler,
        meta_handler: BlockHandler,
    ) {
        self.pages.push(PageIndex {
            file_id,
            data_handle: data_handler,
            meta_handle: meta_handler,
        });
    }

    fn finish(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(PageIndex::encoded_size() * self.pages.len());
        self.pages
            .iter()
            .for_each(|page_index| page_index.encode(&mut buf));
        buf
    }
}

impl PageIndex {
    #[inline]
    pub(super) const fn encoded_size() -> usize {
        core::mem::size_of::<u32>() + BlockHandler::encoded_size() * 2
    }

    #[inline]
    fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::encoded_size());
        self.encode(&mut buf);
        buf
    }

    #[inline]
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.file_id.to_le_bytes());
        self.data_handle.encode(buf);
        self.meta_handle.encode(buf);
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::encoded_size() {
            return Err(Error::Corrupted);
        }

        let idx = 0;
        let end = core::mem::size_of::<u32>();
        let file_id = u32::from_le_bytes(bytes[idx..end].try_into().map_err(|_| Error::Corrupted)?);

        let idx = end;
        let end = idx + BlockHandler::encoded_size();
        let data_handler = BlockHandler::decode(&bytes[idx..end])?;

        let idx = end;
        let end = idx + BlockHandler::encoded_size();
        let meta_handler = BlockHandler::decode(&bytes[idx..end])?;

        Ok(PageIndex {
            file_id,
            data_handle: data_handler,
            meta_handle: meta_handler,
        })
    }
}

impl Footer {
    #[inline]
    pub(super) const fn encoded_size() -> usize {
        core::mem::size_of::<u64>() + BlockHandler::encoded_size() * 2 + 2
    }

    #[inline]
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::encoded_size());
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        self.page_index_handle.encode(&mut bytes);
        self.dealloc_pages_handle.encode(&mut bytes);
        bytes.push(self.compression.bits());
        bytes.push(self.checksum_type.bits());
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::encoded_size() {
            return Err(Error::Corrupted);
        }

        let idx = 0;
        let end = core::mem::size_of::<u64>();
        let magic = u64::from_le_bytes(bytes[idx..end].try_into().map_err(|_| Error::Corrupted)?);

        let idx = end;
        let end = idx + BlockHandler::encoded_size();
        let page_index_handle = BlockHandler::decode(&bytes[idx..end])?;

        let idx = end;
        let end = idx + BlockHandler::encoded_size();
        let dealloc_pages_handle = BlockHandler::decode(&bytes[idx..end])?;

        let compression = Compression::from_bits(bytes[end]).ok_or(Error::Corrupted)?;
        let checksum_type = ChecksumType::from_bits(bytes[end + 1]).ok_or(Error::Corrupted)?;

        Ok(Self {
            magic,
            page_index_handle,
            dealloc_pages_handle,
            compression,
            checksum_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::Env;

    #[test]
    fn footer_encode_and_decode() {
        let footer = Footer {
            magic: 123,
            page_index_handle: BlockHandler {
                offset: 1234,
                length: 64234,
            },
            dealloc_pages_handle: BlockHandler {
                offset: 1231231,
                length: 123,
            },
            compression: Compression::NONE,
            checksum_type: ChecksumType::NONE,
        };

        let payload = footer.encode();
        let new = Footer::decode(&payload).unwrap();
        assert_eq!(new, footer);
    }

    #[test]
    fn page_index_encode_and_decode() {
        let page_index = PageIndex {
            file_id: 123,
            data_handle: BlockHandler {
                offset: 5632,
                length: 123,
            },
            meta_handle: BlockHandler {
                offset: 999,
                length: 123,
            },
        };
        let payload = page_index.encode_to_vec();
        let new = PageIndex::decode(&payload).unwrap();
        assert_eq!(new, page_index);
    }

    #[photonio::test]
    async fn map_file_builder_basic() {
        use tempdir::TempDir;

        use crate::env::Photon;

        let env = Photon;

        let use_direct = false;
        let base_dir = TempDir::new("map_file_builder_basic").unwrap();
        let path1 = base_dir.path().join("buf_test1");
        let base = env.open_dir(base_dir.path()).await.unwrap();

        // Write page file {1, 2, 3} into map file 1.
        let file = env
            .open_sequential_writer(path1.to_owned())
            .await
            .expect("open file_id: {file_id}'s file fail");
        let builder = MapFileBuilder::<Photon>::new(
            1,
            &base,
            file,
            use_direct,
            4096,
            Compression::ZSTD,
            ChecksumType::CRC32,
        );

        // Add page file 1.
        let mut file_builder = builder.add_file(1);
        file_builder
            .add_page(1, 1, empty_page_info(), &[])
            .await
            .unwrap();

        let builder = file_builder.finish().await.unwrap();

        // Add page file 2.
        let mut file_builder = builder.add_file(2);
        file_builder
            .add_page(1, 1, empty_page_info(), &[])
            .await
            .unwrap();

        let builder = file_builder.finish().await.unwrap();

        // Add page file 3.
        let mut file_builder = builder.add_file(3);
        file_builder
            .add_page(1, 1, empty_page_info(), &[])
            .await
            .unwrap();

        let mut builder = file_builder.finish().await.unwrap();
        builder.finish(1).await.unwrap();
    }

    fn empty_page_info() -> PageInfo {
        PageInfo::from_raw(0, 0, 0)
    }
}
