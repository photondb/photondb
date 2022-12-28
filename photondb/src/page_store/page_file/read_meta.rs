use std::{collections::BTreeMap, sync::Arc};

use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    file_builder::IndexBlock,
    file_reader::FileReader,
    map_file_builder::{Footer, PageIndex},
    types::FileMeta,
    PageGroupMeta,
};
use crate::{
    env::PositionalReader,
    page_store::{Error, Result},
};

pub(crate) struct FileMetaHolder {
    /// The file meta of file.
    pub(crate) file_meta: Arc<FileMeta>,
    /// The meta of page groups.
    pub(crate) page_groups: FxHashMap<u32, Arc<PageGroupMeta>>,
    /// The page tables of page groups.
    pub(crate) page_tables: FxHashMap<u32, BTreeMap<u64, u64>>,
    /// The dealloc pages.
    pub(crate) dealloc_pages: Vec<u64>,
}

impl FileMetaHolder {
    /// Open a meta reader with the specified file id.
    pub(crate) async fn read<R: PositionalReader>(
        file_id: u32,
        reader: Arc<FileReader<R>>,
    ) -> Result<Self> {
        let footer = Self::read_footer(&reader).await?;
        let page_indexes = Self::read_page_indexes(&reader, &footer).await?;
        let mut file_meta_map = FxHashMap::default();
        let mut page_tables = FxHashMap::default();
        let mut offset = 0;
        for page_index in &page_indexes {
            let index_block = Self::read_page_group_index_block(&reader, page_index).await?;
            let (indexes, offsets) = index_block.as_meta_file_cached(page_index.data_handle);
            let file_meta =
                PageGroupMeta::new(page_index.file_id, file_id, offset, indexes, offsets);
            let page_table = Self::read_page_table(&reader, &file_meta).await?;
            file_meta_map.insert(page_index.file_id, Arc::new(file_meta));
            page_tables.insert(page_index.file_id, page_table);
            offset = page_index.meta_handle.offset + page_index.meta_handle.length;
        }
        let dealloc_pages = Self::read_dealloc_pages(&reader, &footer).await?;

        let mut referenced_groups = FxHashSet::default();
        if !dealloc_pages.is_empty() {
            for page_addr in &dealloc_pages {
                referenced_groups.insert((page_addr >> 32) as u32);
            }
        }
        let file_meta = Arc::new(FileMeta::new(
            file_id,
            reader.file_size,
            reader.align_size,
            footer.checksum_type,
            footer.compression,
            referenced_groups,
            file_meta_map.clone(),
        ));
        Ok(FileMetaHolder {
            page_groups: file_meta_map,
            file_meta,
            page_tables,
            dealloc_pages,
        })
    }

    /// Read the page table of the specified page group.
    async fn read_page_table<R: PositionalReader>(
        reader: &FileReader<R>,
        group_meta: &PageGroupMeta,
    ) -> Result<BTreeMap<u64, u64>> {
        use super::file_builder::PageTable;

        let block_handle = group_meta.get_page_table_meta_page();
        let buf = reader.read_block(block_handle).await?;
        let table = PageTable::decode(&buf)?;
        Ok(table.into())
    }

    /// Read the [`IndexBlock`] of the specified page group.
    async fn read_page_group_index_block<R: PositionalReader>(
        reader: &FileReader<R>,
        page_index: &PageIndex,
    ) -> Result<IndexBlock> {
        let data_block = reader.read_block(page_index.data_handle).await?;
        let meta_block = reader.read_block(page_index.meta_handle).await?;
        IndexBlock::decode(&data_block, &meta_block)
    }

    /// Read [`Footer`] according to file reader.
    async fn read_footer<R: PositionalReader>(reader: &FileReader<R>) -> Result<Footer> {
        let file_size = reader.file_size;
        if file_size < Footer::encoded_size() {
            return Err(Error::Corrupted);
        }

        let footer_offset = (file_size - Footer::encoded_size()) as u64;
        let mut buf = vec![0u8; Footer::encoded_size() as usize];
        reader.read_exact_at(&mut buf, footer_offset).await?;
        Footer::decode(&buf)
    }

    /// Read [`PageIndex`] of the corresponding file, according to the file
    /// reader.
    async fn read_page_indexes<R: PositionalReader>(
        reader: &FileReader<R>,
        footer: &Footer,
    ) -> Result<Vec<PageIndex>> {
        const RECORD_SIZE: usize = PageIndex::encoded_size();

        let handle = footer.page_index_handle;
        let mut buf = reader.read_block(handle).await?;
        let mut buf = buf.as_mut_slice();
        let mut indexes = Vec::default();
        while !buf.is_empty() {
            let payload = &buf[..RECORD_SIZE];
            let page_index = PageIndex::decode(payload)?;
            indexes.push(page_index);
            buf = &mut buf[RECORD_SIZE..];
        }
        Ok(indexes)
    }

    /// Read the dealloc pages block.
    async fn read_dealloc_pages<R: PositionalReader>(
        reader: &FileReader<R>,
        footer: &Footer,
    ) -> Result<Vec<u64>> {
        let handle = footer.dealloc_pages_handle;
        let mut buf = reader.read_block(handle).await?;
        let mut buf = buf.as_mut_slice();
        let mut dealloc_pages = Vec::default();
        while !buf.is_empty() {
            let mut payload = [0u8; core::mem::size_of::<u64>()];
            payload.copy_from_slice(&buf[..core::mem::size_of::<u64>()]);
            dealloc_pages.push(u64::from_le_bytes(payload));
            buf = &mut buf[core::mem::size_of::<u64>()..];
        }
        Ok(dealloc_pages)
    }
}
