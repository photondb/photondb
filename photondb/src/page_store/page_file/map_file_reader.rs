use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use super::{
    file_builder::IndexBlock,
    file_reader::CommonFileReader,
    map_file_builder::{Footer, PageIndex},
    types::MapFileMeta,
    BlockHandler, FileMeta,
};
use crate::{
    env::PositionalReader,
    page_store::{Error, Result},
};

pub(crate) type MapFileReader<R> = CommonFileReader<R>;

pub(crate) struct MapFileMetaHolder {
    /// The file meta of map file.
    pub(crate) file_meta: Arc<MapFileMeta>,
    /// The meta of page files.
    pub(crate) file_meta_map: HashMap<u32, Arc<FileMeta>>,
    /// The page tables of page files.
    pub(crate) page_tables: HashMap<u32, BTreeMap<u64, u64>>,
    /// The offset of first byte of page files.
    file_offsets: HashMap<u32, u64>,
}

impl MapFileMetaHolder {
    /// Return the offset of the specified file, if exists.
    pub(crate) fn file_offset(&self, file_id: u32) -> Option<u64> {
        self.file_offsets.get(&file_id).cloned()
    }

    /// Open a meta reader with the specified map file id.
    pub(crate) async fn read<R: PositionalReader>(
        file_id: u32,
        reader: Arc<MapFileReader<R>>,
    ) -> Result<Self> {
        let footer = Self::read_footer(&reader).await?;
        let file_indexes = Self::read_file_indexes(&reader, &footer).await?;
        let mut file_meta_map = HashMap::default();
        let mut page_tables = HashMap::default();
        let mut offset = 0;
        let mut file_offsets = HashMap::with_capacity(file_indexes.len());
        for page_index in &file_indexes {
            file_offsets.insert(page_index.file_id, offset);
            offset = page_index.meta_handle.offset + page_index.meta_handle.length;
            let index_block = Self::read_partial_file_index_block(&reader, page_index).await?;
            let (indexes, offsets) = index_block.as_meta_file_cached(page_index.data_handle);
            let file_meta = FileMeta::new_partial(
                page_index.file_id,
                file_id,
                reader.align_size,
                indexes,
                offsets,
            );
            let page_table = Self::read_page_table(&reader, &file_meta).await?;
            file_meta_map.insert(page_index.file_id, Arc::new(file_meta));
            page_tables.insert(page_index.file_id, page_table);
        }
        let file_meta = Arc::new(MapFileMeta::new(
            file_id,
            reader.file_size,
            reader.align_size,
            file_meta_map.clone(),
        ));
        Ok(MapFileMetaHolder {
            file_offsets,
            file_meta_map,
            file_meta,
            page_tables,
        })
    }

    /// Read the page table of the specified partital file.
    async fn read_page_table<R: PositionalReader>(
        reader: &MapFileReader<R>,
        file_meta: &FileMeta,
    ) -> Result<BTreeMap<u64, u64>> {
        use super::file_builder::PageTable;

        let (offset, length) = file_meta.get_page_table_meta_page()?;
        let length = length as u64;
        let block_handle = BlockHandler { offset, length };
        let buf = reader.read_block(block_handle).await?;
        let table = PageTable::decode(&buf)?;
        Ok(table.into())
    }

    /// Read the [`IndexBlock`] of the specified partial file.
    async fn read_partial_file_index_block<R: PositionalReader>(
        reader: &MapFileReader<R>,
        page_index: &PageIndex,
    ) -> Result<IndexBlock> {
        let data_block = reader.read_block(page_index.data_handle).await?;
        let meta_block = reader.read_block(page_index.meta_handle).await?;
        IndexBlock::decode(&data_block, &meta_block)
    }

    /// Read [`Footer`] according to file reader.
    async fn read_footer<R: PositionalReader>(reader: &MapFileReader<R>) -> Result<Footer> {
        let file_size = reader.file_size;
        if file_size < Footer::encoded_size() {
            return Err(Error::Corrupted);
        }

        let footer_offset = (file_size - Footer::encoded_size()) as u64;
        let mut buf = vec![0u8; Footer::encoded_size() as usize];
        reader.read_exact_at(&mut buf, footer_offset).await?;
        Footer::decode(&buf)
    }

    /// Read [`PageIndex`] of the corresponding map file, according to the file
    /// reader.
    async fn read_file_indexes<R: PositionalReader>(
        reader: &MapFileReader<R>,
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
}
