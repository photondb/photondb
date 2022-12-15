use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use futures::Future;

use super::{file_builder::*, types::FileMeta, FileId};
use crate::{
    env::{Env, PositionalReader, PositionalReaderExt},
    page_store::{cache::Cache, stats::CacheStats, ClockCache, Error, Result},
    util::atomic::Counter,
};

pub(crate) struct CommonFileReader<R: PositionalReader> {
    reader: R,
    use_direct: bool,
    pub(super) align_size: usize,
    pub(super) file_size: usize,
    read_bytes: Counter,
}

impl<R: PositionalReader> CommonFileReader<R> {
    /// Open page reader.
    pub(super) fn from(reader: R, use_direct: bool, align_size: usize, file_size: usize) -> Self {
        Self {
            reader,
            use_direct,
            align_size,
            file_size,
            read_bytes: Counter::new(0),
        }
    }

    /// Reads the exact number of bytes from the page specified by `offset`.
    pub(crate) async fn read_exact_at(&self, buf: &mut [u8], req_offset: u64) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        if !self.use_direct {
            self.reader
                .read_exact_at(buf, req_offset)
                .await
                .expect("read page data fail");
            self.read_bytes.add(buf.len() as u64);
            return Ok(());
        }

        let align_offset = floor_to_block_lo_pos(req_offset as usize, self.align_size);
        let offset_ahead = (req_offset as usize) - align_offset;
        let align_buf_size =
            ceil_to_block_hi_pos(req_offset as usize + buf.len(), self.align_size) - align_offset;

        let mut align_buf = AlignBuffer::new(align_buf_size, self.align_size); // TODO: pool this buf?
        let read_buf = align_buf.as_bytes_mut();

        self.inner_read_exact_at(&self.reader, read_buf, align_offset as u64)
            .await
            .expect("read page data fail");

        buf.copy_from_slice(&read_buf[offset_ahead..offset_ahead + buf.len()]);
        self.read_bytes.add(buf.len() as u64);

        Ok(())
    }

    async fn inner_read_exact_at(
        &self,
        r: &R,
        mut buf: &mut [u8],
        mut pos: u64,
    ) -> std::io::Result<()> {
        assert!(is_block_aligned_ptr(buf.as_ptr(), self.align_size));
        assert!(is_block_aligned_pos(pos as usize, self.align_size));
        while !buf.is_empty() {
            match r.read_at(buf, pos).await {
                Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    buf = &mut buf[n..];
                    pos += n as u64;
                    if !is_block_aligned_pos(n, self.align_size) {
                        // only happen when end of file.
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub(crate) async fn read_block(&self, block_handle: BlockHandler) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; block_handle.length as usize];
        self.read_exact_at(&mut buf, block_handle.offset).await?;
        Ok(buf)
    }

    #[inline]
    pub(crate) fn total_read_bytes(&self) -> u64 {
        self.read_bytes.get()
    }
}

pub(crate) type PageFileReader<R> = CommonFileReader<R>;

pub(crate) struct MetaReader<R: PositionalReader> {
    reader: Arc<PageFileReader<R>>,
    file_meta: Arc<FileMeta>,
}

impl<R: PositionalReader> MetaReader<R> {
    // Returns file_meta by read file's footer and index_block.
    pub(crate) async fn read_file_meta(
        reader: &PageFileReader<R>,
        file_id: u32,
    ) -> Result<Arc<FileMeta>> {
        let footer = Self::read_footer(reader).await?;
        let index_block = Self::read_index_block(reader, &footer).await?;
        Ok({
            let (indexes, offsets) = index_block.as_meta_file_cached(footer.data_handle);
            Arc::new(FileMeta::new(
                file_id,
                reader.file_size,
                reader.align_size,
                indexes,
                offsets,
                footer.compression,
                footer.checksum_type,
            ))
        })
    }

    /// Open reader to read meta pages in the file.
    pub(crate) async fn open(reader: Arc<PageFileReader<R>>, file_id: u32) -> Result<Self> {
        let file_meta = Self::read_file_meta(&reader, file_id).await?;
        Ok(Self::with_file_meta(reader, file_meta))
    }

    pub(crate) fn with_file_meta(reader: Arc<PageFileReader<R>>, file_meta: Arc<FileMeta>) -> Self {
        MetaReader { reader, file_meta }
    }

    /// Returns the page table in the file.
    pub(crate) async fn read_page_table(&self) -> Result<BTreeMap<u64, u64>> {
        let (page_table_offset, page_table_len) = self.file_meta.get_page_table_meta_page()?;
        let mut buf = vec![0u8; page_table_len];
        self.reader
            .read_exact_at(&mut buf, page_table_offset)
            .await
            .expect("read page table meta page fail");
        let table = PageTable::decode(&buf)?;
        Ok(table.into())
    }

    /// Returns the delete page addrs in the file.
    pub(crate) async fn read_delete_pages(&self) -> Result<Vec<u64> /* delete page addrs */> {
        let (del_offset, del_len) = self.file_meta.get_delete_pages_meta_page()?;
        let mut buf = vec![0u8; del_len];
        self.reader
            .read_exact_at(&mut buf, del_offset)
            .await
            .expect("read delete pages meta page fail");
        let dels = DeletePages::decode(&buf)?;
        Ok(dels.into())
    }

    //// Returns the file metadata for current reader.
    pub(crate) fn file_metadata(&self) -> Arc<FileMeta> {
        self.file_meta.clone()
    }

    #[inline]
    pub(crate) fn into_inner(self) -> Arc<PageFileReader<R>> {
        self.reader
    }
}

impl<R: PositionalReader> MetaReader<R> {
    async fn read_footer(reader: &PageFileReader<R>) -> Result<Footer> {
        let file_size = reader.file_size as u32;
        if file_size <= Footer::size() {
            return Err(Error::Corrupted);
        }
        let footer_offset = (file_size - Footer::size()) as u64;
        let mut buf = vec![0u8; Footer::size() as usize];
        reader
            .read_exact_at(&mut buf, footer_offset)
            .await
            .expect("read file footer error");
        let footer = Footer::decode(&buf)?;
        Ok(footer)
    }

    async fn read_index_block(read: &PageFileReader<R>, footer: &Footer) -> Result<IndexBlock> {
        let mut data_idx_bytes = vec![0u8; footer.data_handle.length as usize];
        read.read_exact_at(&mut data_idx_bytes, footer.data_handle.offset)
            .await
            .expect("read data page index fail");

        let mut meta_idx_bytes = vec![0u8; footer.meta_handle.length as usize];
        read.read_exact_at(&mut meta_idx_bytes, footer.meta_handle.offset)
            .await
            .expect("read meta page index fail");

        IndexBlock::decode(&data_idx_bytes, &meta_idx_bytes)
    }
}

pub(super) struct FileReaderCache<E: Env> {
    cache: Arc<ClockCache<Arc<PageFileReader<E::PositionalReader>>>>,
    _marker: PhantomData<E>,
}

impl<E: Env> FileReaderCache<E> {
    pub(super) fn new(max_size: u64) -> Self {
        let cache = Arc::new(ClockCache::new(max_size as usize, 1, -1, false, false));
        Self {
            cache,
            _marker: PhantomData,
        }
    }

    pub(super) async fn get_with(
        &self,
        file_id: FileId,
        init: impl Future<Output = Arc<PageFileReader<E::PositionalReader>>>,
    ) -> Result<Arc<PageFileReader<E::PositionalReader>>> {
        let key = Self::file_id_to_key(file_id);
        if let Some(cached) = self.cache.lookup(key) {
            return Ok(cached.value().clone());
        }
        let reader = init.await;
        match self.cache.insert(key, Some(reader.clone()), 1) {
            Ok(_) | Err(Error::MemoryLimit) => {}
            Err(err) => return Err(err),
        }
        Ok(reader)
    }

    pub(super) fn invalidate(&self, file_id: FileId) {
        let key = Self::file_id_to_key(file_id);
        self.cache.erase(key);
    }

    pub(super) fn stats(&self) -> CacheStats {
        self.cache.stats()
    }

    #[inline]
    fn file_id_to_key(file_id: FileId) -> u64 {
        match file_id {
            FileId::Page(id) => id as u64,
            FileId::Map(id) => (1u64 << 32) | (id as u64),
        }
    }
}
