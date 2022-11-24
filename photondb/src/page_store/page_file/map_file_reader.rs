use std::{collections::HashMap, sync::Arc};

use super::{
    file_builder::IndexBlock,
    file_reader::CommonFileReader,
    map_file_builder::{Footer, PageIndex},
    types::MapFileMeta,
    FileMeta,
};
use crate::{
    env::PositionalReader,
    page_store::{Error, Result},
};

pub(crate) type MapFileReader<R> = CommonFileReader<R>;

#[allow(unused)]
pub(crate) struct MapFileMetaReader<R: PositionalReader> {
    /// The id of map file.
    file_id: u32,
    /// The underlying file reader.
    reader: Arc<CommonFileReader<R>>,
    /// The file meta of map file.
    file_meta: Arc<MapFileMeta>,
    /// The block handle indexes of page files.
    file_indexes: HashMap<u32, PageIndex>,
    /// The index blocks of page files.
    index_blocks: HashMap<u32, IndexBlock>,
    /// The meta of page files.
    file_meta_map: HashMap<u32, Arc<FileMeta>>,
}

impl<R: PositionalReader> MapFileMetaReader<R> {
    /// Open a meta reader with the specified map file id.
    pub(crate) async fn open(file_id: u32, reader: Arc<MapFileReader<R>>) -> Result<Self> {
        let footer = Self::read_footer(&reader).await?;
        let file_indexes = Self::read_file_indexes(&reader, &footer).await?;
        let mut index_blocks = HashMap::default();
        let mut file_meta_map = HashMap::default();
        for page_index in file_indexes.values() {
            let index_block = Self::read_partial_file_index_block(&reader, page_index).await?;
            let (indexes, offsets) = index_block.as_meta_file_cached(page_index.data_handle);
            let file_meta = FileMeta::new_partial(
                page_index.file_id,
                file_id,
                reader.align_size,
                indexes,
                offsets,
            );
            index_blocks.insert(page_index.file_id, index_block);
            file_meta_map.insert(page_index.file_id, Arc::new(file_meta));
        }
        let file_meta = Arc::new(MapFileMeta::new(
            file_id,
            reader.file_size,
            reader.align_size,
            file_meta_map.clone(),
        ));
        Ok(MapFileMetaReader {
            file_id,
            reader,
            file_indexes,
            index_blocks,
            file_meta_map,
            file_meta,
        })
    }

    /// Get the [`MapFileMeta`].
    pub(crate) fn file_meta(&self) -> Arc<MapFileMeta> {
        self.file_meta.clone()
    }

    /// Get the [`FileMeta`] for partial files.
    pub(crate) fn page_file_meta_map(&self) -> &HashMap<u32, Arc<FileMeta>> {
        &self.file_meta_map
    }

    /// Read the [`IndexBlock`] of the specified partial file.
    async fn read_partial_file_index_block(
        reader: &MapFileReader<R>,
        page_index: &PageIndex,
    ) -> Result<IndexBlock> {
        let data_block = reader.read_block(page_index.data_handle).await?;
        let meta_block = reader.read_block(page_index.meta_handle).await?;
        IndexBlock::decode(&data_block, &meta_block)
    }

    /// Read [`Footer`] according to file reader.
    async fn read_footer(reader: &MapFileReader<R>) -> Result<Footer> {
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
    async fn read_file_indexes(
        reader: &MapFileReader<R>,
        footer: &Footer,
    ) -> Result<HashMap<u32, PageIndex>> {
        const RECORD_SIZE: usize = PageIndex::encoded_size();

        let handle = footer.page_index_handle;
        let mut buf = reader.read_block(handle).await?;
        let mut buf = buf.as_mut_slice();
        let mut indexes = HashMap::new();
        while !buf.is_empty() {
            let payload = &buf[..RECORD_SIZE];
            let page_index = PageIndex::decode(payload)?;
            indexes.insert(page_index.file_id, page_index);
            buf = &mut buf[RECORD_SIZE..];
        }
        Ok(indexes)
    }
}
