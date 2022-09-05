use bytes::{Buf, BufMut};

use crate::{
    env::{PositionalRead, PositionalReadExt, SequentialWrite, SequentialWriteExt},
    tree::{Error, Result},
};

/// A magic number generated with `echo photondb.page.v1 | shasum | cut -c -16`.
const PAGE_FILE_MAGIC: u64 = 0x4413c5fc30a55826;

struct Handle {
    pub offset: u64,
    pub length: u64,
}

impl Handle {
    const ENCODED_LEN: usize = 16;

    fn encode_to<B: BufMut>(&self, mut buf: B) {
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.length);
    }

    fn decode_from<B: Buf>(mut buf: B) -> Self {
        let offset = buf.get_u64_le();
        let length = buf.get_u64_le();
        Self { offset, length }
    }
}

struct Footer {
    data_index_handle: Handle,
    meta_index_handle: Handle,
    magic_number: u64,
}

impl Footer {
    const ENCODED_LEN: usize = Handle::ENCODED_LEN * 2 + 8;

    fn encode_to<B: BufMut>(&self, mut buf: B) {
        self.data_index_handle.encode_to(&mut buf);
        self.meta_index_handle.encode_to(&mut buf);
        buf.put_u64_le(self.magic_number);
    }

    fn decode_from<B: Buf>(mut buf: B) -> Self {
        let data_index_handle = Handle::decode_from(&mut buf);
        let meta_index_handle = Handle::decode_from(&mut buf);
        let magic_number = buf.get_u64_le();
        Self {
            data_index_handle,
            meta_index_handle,
            magic_number,
        }
    }
}

pub struct PageFile<R> {
    file: R,
}

impl<R: PositionalRead> PageFile<R> {
    pub async fn open(file: R, size: u64) -> Result<Self> {
        if size < Footer::ENCODED_LEN as u64 {
            return Err(Error::Corrupted(format!("page file is too small")));
        }
        let mut footer_block = [0; Footer::ENCODED_LEN];
        file.read_exact_at(&mut footer_block, size - Footer::ENCODED_LEN as u64)
            .await?;
        let footer = Footer::decode_from(footer_block.as_slice());
        if footer.magic_number != PAGE_FILE_MAGIC {
            return Err(Error::Corrupted(format!(
                "page file has invalid magic number"
            )));
        }
        Ok(Self { file })
    }
}

pub struct PageFileBuilder<W> {
    file: W,
    offset: u64,
    page_head_block: PageHeadBlockBuilder,
    data_index_block: DataIndexBlockBuilder,
}

impl<W: SequentialWrite> PageFileBuilder<W> {
    async fn add_page(&mut self, psn: u64, head: PageHead, data: &[u8]) -> Result<()> {
        // self.page_head_block.add(head);
        let handle = self.append(data).await?;
        self.data_index_block.add(psn, handle.offset);
        Ok(())
    }

    async fn append(&mut self, buf: &[u8]) -> Result<Handle> {
        self.file.write_all(buf).await?;
        let handle = Handle {
            offset: self.offset,
            length: buf.len() as u64,
        };
        self.offset += handle.length;
        Ok(handle)
    }

    async fn finish(mut self) -> Result<()> {
        let page_head_block = self.page_head_block.finish();
        let page_head_handle = self.append(&page_head_block).await?;
        let data_index_block = self.data_index_block.finish();
        let data_index_handle = self.append(&data_index_block).await?;

        let mut meta_index_block = MetaIndexBlockBuilder::default();
        meta_index_block.add_page_head(page_head_handle);
        let meta_index_block = meta_index_block.finish();
        let meta_index_handle = self.append(&meta_index_block).await?;

        let footer = Footer {
            data_index_handle,
            meta_index_handle,
            magic_number: PAGE_FILE_MAGIC,
        };
        let mut footer_block = [0; Footer::ENCODED_LEN];
        footer.encode_to(footer_block.as_mut_slice());
        self.append(&footer_block).await?;

        Ok(())
    }
}

struct PageHead {}

#[derive(Default)]
struct PageHeadBlockBuilder {
    buf: Vec<u8>,
}

impl PageHeadBlockBuilder {
    fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }
}

#[derive(Default)]
struct DataIndexBlockBuilder {
    buf: Vec<u8>,
}

impl DataIndexBlockBuilder {
    fn add(&mut self, psn: u64, offset: u64) {
        todo!()
    }

    fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }
}

#[derive(Default)]
struct MetaIndexBlockBuilder {
    buf: Vec<u8>,
}

impl MetaIndexBlockBuilder {
    fn add_page_head(&mut self, handle: Handle) {
        todo!()
    }

    fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }
}
