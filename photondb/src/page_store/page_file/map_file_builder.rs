#![allow(unused)]
use std::collections::HashMap;

use super::{constant::*, BlockHandler, BufferedWriter, CommonFileBuilder, FileInfo};
use crate::{
    env::Env,
    page_store::{Error, Result},
};

/// Builder for map file.
///
/// File format:
///
/// File = [{page file}] {page block index} {footer}
/// page file = {data blocks} {meta blocks} {index blocks}
/// data blocks = [{data block}] --- one block per tree page
/// meta blocks = {page table block}
/// page table block = [(page_id, page_addr[low 32bit])]
/// index_blocks = {data block index} {meta block index}
/// data block index = {page_addr[low 32bit], file_offset}
/// meta block index = {file_offset}
/// page block index = [(page_id, {data block index}, {meta block index})]
/// footer = {magic_number} { page block index}
pub(crate) struct MapFileBuilder<'a, E: Env> {
    file_id: u32,
    writer: BufferedWriter<'a, E>,
    page_index: PageIndexBuilder,
    file_infos: HashMap<u32, FileInfo>,
    block_size: usize,
}

/// File builder for partial of map file.
pub(crate) struct PartialFileBuilder<'a, E: Env> {
    file_id: u32,
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
struct PageIndex {
    file_id: u32,
    data_handler: BlockHandler,
    meta_handler: BlockHandler,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Footer {
    magic: u64,
    page_index_handler: BlockHandler,
}

impl<'a, E: Env> MapFileBuilder<'a, E> {
    pub(crate) fn new(
        file_id: u32,
        base_dir: &'a E::Directory,
        file: E::SequentialWriter,
        use_direct: bool,
        block_size: usize,
    ) -> Self {
        let writer = BufferedWriter::new(file, IO_BUFFER_SIZE, use_direct, block_size, base_dir);
        Self {
            file_id,
            writer,
            page_index: PageIndexBuilder::default(),
            file_infos: HashMap::default(),
            block_size,
        }
    }

    pub(crate) fn add_file(self, file_id: u32) -> PartialFileBuilder<'a, E> {
        let block_size = self.block_size;
        PartialFileBuilder {
            file_id,
            builder: self,
            inner: CommonFileBuilder::new(file_id, block_size),
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<()> {
        let page_index_block = self.page_index.finish();
        let offset = self.writer.write(&page_index_block).await?;
        let length = page_index_block.len() as u64;
        let page_index_handler = BlockHandler { offset, length };
        let footer = Footer {
            magic: MAP_FILE_MAGIC,
            page_index_handler,
        };
        let payload = footer.encode();
        self.writer.write(&payload).await?;
        Ok(())
    }
}

impl<'a, E: Env> PartialFileBuilder<'a, E> {
    /// Add a new page to builder.
    pub(crate) async fn add_page(
        &mut self,
        page_id: u64,
        page_addr: u64,
        page_content: &[u8],
    ) -> Result<()> {
        self.inner
            .add_page(&mut self.builder.writer, page_id, page_addr, page_content)
            .await
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
        // TODO(walter) add file info.
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
            data_handler,
            meta_handler,
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
    fn encoded_size() -> usize {
        core::mem::size_of::<u32>() + core::mem::size_of::<u64>() * 4
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
        self.data_handler.encode(buf);
        self.meta_handler.encode(buf);
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
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
            data_handler,
            meta_handler,
        })
    }
}

impl Footer {
    #[inline]
    fn encoded_size() -> usize {
        core::mem::size_of::<u64>() * 3
    }

    #[inline]
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::encoded_size());
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        self.page_index_handler.encode(&mut bytes);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::encoded_size() {
            return Err(Error::Corrupted);
        }

        let idx = 0;
        let end = core::mem::size_of::<u64>();
        let magic = u64::from_le_bytes(bytes[idx..end].try_into().map_err(|_| Error::Corrupted)?);

        let idx = end;
        let end = idx + BlockHandler::encoded_size();
        let page_index_handler = BlockHandler::decode(&bytes[idx..end])?;
        Ok(Self {
            magic,
            page_index_handler,
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
            page_index_handler: BlockHandler {
                offset: 1234,
                length: 64234,
            },
        };

        let payload = footer.encode();
        let new = Footer::decode(&payload).unwrap();
        assert_eq!(new, footer);
    }

    #[test]
    fn page_index_encode_and_decode() {
        let page_index = PageIndex {
            file_id: 123,
            data_handler: BlockHandler {
                offset: 5632,
                length: 123,
            },
            meta_handler: BlockHandler {
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
        let builder = MapFileBuilder::<Photon>::new(1, &base, file, use_direct, 4096);

        // Add page file 1.
        let mut file_builder = builder.add_file(1);
        file_builder.add_page(1, 1, &[]).await.unwrap();

        let builder = file_builder.finish().await.unwrap();

        // Add page file 2.
        let mut file_builder = builder.add_file(2);
        file_builder.add_page(1, 1, &[]).await.unwrap();

        let builder = file_builder.finish().await.unwrap();

        // Add page file 3.
        let mut file_builder = builder.add_file(3);
        file_builder.add_page(1, 1, &[]).await.unwrap();

        let mut builder = file_builder.finish().await.unwrap();
        builder.finish().await.unwrap();
    }
}
