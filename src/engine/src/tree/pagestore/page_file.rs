use std::io::{Result, Write};

use crate::env::PositionalRead;

// Page file format:
//
// page_file = data_block { meta_block } meta_index_block footer
// data_block = { page }
// meta_block = gist_block | index_block

struct Footer {
    /// Points to the meta index block.
    pub index_handle: BlockHandle,
    pub magic_number: u64,
}

struct BlockHandle {
    pub offset: u64,
    pub length: u64,
}

pub struct PageFile<R> {
    file: R,
}

impl<R: PositionalRead> PageFile<R> {
    pub fn offset(&self) -> u64 {
        todo!()
    }

    pub fn num_pages(&self) -> usize {
        todo!()
    }

    pub fn page_head(&self, offset: u64) {
        todo!()
    }

    pub async fn read_page(&self, offset: u64) {
        todo!()
    }
}

pub struct PageBuf;

impl PageBuf {
    fn gist(&self) -> PageGist {
        todo!()
    }

    fn data(&self) -> &[u8] {
        todo!()
    }

    fn size(&self) -> usize {
        todo!()
    }
}

pub struct PageGist;

pub struct PageFileBuilder<W> {
    file: W,
    gist: GistBlockBuilder,
    index: IndexBlockBuilder,
    offset: u64,
}

impl<W: Write> PageFileBuilder<W> {
    fn add_page(&mut self, id: u64, page: PageBuf) -> Result<()> {
        self.gist.add(page.gist());
        self.index.add(id, self.offset);
        self.append(page.data())
    }

    fn append(&mut self, buf: &[u8]) -> Result<()> {
        self.file.write_all(buf)?;
        self.offset += buf.len() as u64;
        Ok(())
    }

    fn finish(self) -> Result<()> {
        let mut meta_index = MetaIndexBlockBuilder::default();

        let gist_block = self.gist.finish();
        let gist_handle = BlockHandle {
            offset: self.offset,
            length: gist_block.len() as u64,
        };
        self.append(&gist_block)?;
        meta_index.add_gist(gist_handle);

        let index_block = self.index.finish();
        let index_handle = BlockHandle {
            offset: self.offset,
            length: index_block.len() as u64,
        };
        self.append(&index_block)?;
        meta_index.add_index(index_handle);

        let meta_index_block = meta_index.finish();
        let meta_index_handle = BlockHandle {
            offset: self.offset,
            length: meta_index_block.len() as u64,
        };
        self.append(&meta_index_block)?;

        let footer = FileFooter {
            index_handle: meta_index_handle,
            magic_number: 0x12345678,
        };
        self.append(&footer.encode())?;

        self.file.flush()?;
    }
}

struct GistBlockBuilder {
    buf: Vec<u8>,
}

impl GistBlockBuilder {
    fn add(&mut self, gist: PageGist) {
        todo!()
    }

    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

struct IndexBlockBuilder {
    buf: Vec<u8>,
}

impl IndexBlockBuilder {
    fn add(&mut self, id: u64, offset: u64) {
        todo!()
    }

    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

struct MetaIndexBlock {
    gist: BlockHandle,
    index: BlockHandle,
}

#[derive(Default)]
struct MetaIndexBlockBuilder {
    buf: Vec<u8>,
}

impl MetaIndexBlockBuilder {
    fn add_gist(&mut self, handle: BlockHandle) {
        todo!()
    }

    fn add_index(&mut self, handle: BlockHandle) {
        todo!()
    }

    fn finish(self) -> Vec<u8> {
        self.buf
    }
}
