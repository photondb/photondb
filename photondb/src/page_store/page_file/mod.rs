mod cache;

mod file_builder;
pub(self) use file_builder::{BlockHandle, BufferedWriter};

mod file_reader;
pub(crate) use file_reader::FileReader;

mod types;
pub(crate) use facade::PageFiles;
pub(crate) use types::{FileInfo, PageGroup, PageGroupMeta};

mod map_file_builder;
pub(crate) use map_file_builder::{FileBuilder, PageGroupBuilder};

mod read_meta;
pub(crate) use read_meta::FileMetaHolder;

mod compression;
pub use compression::Compression;

mod checksum;
pub use checksum::ChecksumType;

pub(crate) mod constant {
    /// Default alignment requirement for the SSD.
    // TODO: query logical sector size
    // like: https://github.com/DataDog/glommio/issues/7 or https://github.com/facebook/rocksdb/pull/1875
    pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4096;

    pub(crate) const IO_BUFFER_SIZE: usize = 4096 * 4;

    pub(crate) const FILE_MAGIC: u64 = 0x179394;
}

pub(crate) mod facade {
    use std::{path::PathBuf, sync::Arc};

    use super::{
        cache::FileReaderCache,
        constant::DEFAULT_BLOCK_SIZE,
        file_reader::FileReader,
        types::{FileMeta, PageHandle},
        *,
    };
    use crate::{
        env::{Env, PositionalReader, SequentialWriter},
        page_store::{
            page_txn::CacheOption, stats::CacheStats, Cache, CacheEntry, Error, LRUCache, Result,
        },
        PageStoreOptions,
    };

    pub(crate) const FILE_PREFIX: &str = "map";

    /// The facade for page_file module.
    /// it hides the detail about disk location for caller(after it be created).
    pub(crate) struct PageFiles<E: Env> {
        env: E,
        base: PathBuf,
        base_dir: E::Directory,

        use_direct: bool,
        prepopulate_cache_on_flush: bool,

        reader_cache: cache::FileReaderCache<E>,
        page_cache: Arc<LRUCache<Vec<u8>>>,
    }

    impl<E: Env> PageFiles<E> {
        /// Create page file facade.
        /// It should be a singleton in the page_store.
        pub(crate) async fn new(
            env: E,
            base: impl Into<PathBuf>,
            options: &PageStoreOptions,
        ) -> Self {
            let base = base.into();
            let base_dir = env.open_dir(&base).await.expect("open base dir fail");
            let reader_cache = FileReaderCache::new(options.cache_file_reader_capacity);
            let page_cache = Arc::new(LRUCache::new(options.cache_capacity, -1));
            let use_direct = options.use_direct_io;
            let prepopulate_cache_on_flush = options.prepopulate_cache_on_flush;
            Self {
                env,
                base,
                base_dir,
                use_direct,
                prepopulate_cache_on_flush,
                reader_cache,
                page_cache,
            }
        }

        /// Create `MapFileBuilder` to write a new map file.
        pub(crate) async fn new_file_builder(
            &self,
            file_id: u32,
            compression: Compression,
            checksum: ChecksumType,
        ) -> Result<FileBuilder<E>> {
            // TODO: switch to env in suitable time.
            let path = self.base.join(format!("{}_{file_id}", FILE_PREFIX));
            let writer = self
                .env
                .open_sequential_writer(path.to_owned())
                .await
                .expect("open writer for file_id: {file_id} fail");
            let use_direct = self.use_direct && writer.direct_io_ify().is_ok();
            Ok(FileBuilder::new(
                file_id,
                &self.base_dir,
                writer,
                use_direct,
                DEFAULT_BLOCK_SIZE,
                compression,
                checksum,
            ))
        }

        pub(crate) async fn read_page(
            &self,
            file_id: u32,
            file_meta: &FileMeta,
            addr: u64,
            handle: PageHandle,
            hint: CacheOption,
        ) -> Result<(CacheEntry<Vec<u8>, LRUCache<Vec<u8>>>, /* hit */ bool)> {
            if let Some(cache_entry) = self.page_cache.lookup(addr) {
                return Ok((cache_entry, true));
            }

            let buf = self.read_file_page(file_id, file_meta, handle).await?;

            let charge = buf.len();
            let cache_entry = self.page_cache.insert(addr, Some(buf), charge, hint)?;
            Ok((cache_entry.unwrap(), false))
        }

        pub(crate) async fn read_file_page(
            &self,
            file_id: u32,
            file_meta: &FileMeta,
            handle: PageHandle,
        ) -> Result<Vec<u8>> {
            const CHECKSUM_LEN: usize = std::mem::size_of::<u32>();

            let reader = self.open_page_reader(file_id, file_meta.block_size).await?;

            let mut buf = vec![0u8; handle.size as usize]; // TODO: aligned buffer pool
            self.read_file_page_from_reader(&reader, file_meta, handle, &mut buf)
                .await?;
            Ok(buf)
        }

        pub(crate) async fn read_file_page_from_reader(
            &self,
            reader: &FileReader<<E as Env>::PositionalReader>,
            file_meta: &FileMeta,
            handle: PageHandle,
            output: &mut Vec<u8>,
        ) -> Result<()> {
            const CHECKSUM_LEN: usize = std::mem::size_of::<u32>();

            reader.read_exact_at(output, handle.offset as u64).await?;

            if file_meta.checksum_type != ChecksumType::NONE {
                let checksum = u32::from_le_bytes(
                    output[output.len() - CHECKSUM_LEN..output.len()]
                        .try_into()
                        .map_err(|_| Error::Corrupted)?,
                );
                output.truncate(output.len() - CHECKSUM_LEN);
                checksum::check_checksum(file_meta.checksum_type, output, checksum)?;
            }

            let compression = file_meta.compression;
            if compression != Compression::NONE {
                let (decompress_len, skip) = compression::decompress_len(compression, output)?;
                let mut dec_buf = vec![0u8; decompress_len];
                compression::decompress_into(compression, &output[skip..], &mut dec_buf)?;
                if output.len() < dec_buf.len() {
                    output.resize(dec_buf.len(), 0u8);
                }
                output[..dec_buf.len()].copy_from_slice(&dec_buf);
                output.truncate(dec_buf.len());
            }
            Ok(())
        }

        /// Open page_reader for a page_file.
        /// page_store could get file_id & block_size from page_addr's high bit
        /// and version.active_files.
        pub(crate) async fn open_page_reader(
            &self,
            file_id: u32,
            block_size: usize,
        ) -> Result<Arc<FileReader<E::PositionalReader>>> {
            self.reader_cache
                .get_with(file_id, async move {
                    let (prefix, id) = (FILE_PREFIX, file_id);
                    let (file, file_size) = self
                        .open_positional_reader(prefix, id)
                        .await
                        .expect("open reader for file_id: {file_id} fail");
                    let use_direct = self.use_direct && file.direct_io_ify().is_ok();
                    Arc::new(FileReader::from(
                        file,
                        use_direct,
                        block_size,
                        file_size as usize,
                    ))
                })
                .await
        }

        pub(crate) async fn read_file_meta(&self, file_id: u32) -> Result<FileMetaHolder> {
            let (file, file_size) = self.open_positional_reader(FILE_PREFIX, file_id).await?;
            let page_file_reader = Arc::new(FileReader::from(
                file,
                true,
                DEFAULT_BLOCK_SIZE,
                file_size as usize,
            ));
            FileMetaHolder::read(file_id, page_file_reader).await
        }

        async fn open_positional_reader(
            &self,
            prefix: &str,
            file_id: u32,
        ) -> Result<(E::PositionalReader, u64)> {
            let path = self.base.join(format!("{}_{file_id}", prefix));
            let file_size = self
                .env
                .metadata(&path)
                .await
                .expect("read fs metadata fail")
                .len;
            let file = self
                .env
                .open_positional_reader(path)
                .await
                .expect("open reader for file_id: {file_id} fail");
            Ok((file, file_size))
        }

        pub(crate) async fn remove_files(&self, files: Vec<u32>) -> Result<()> {
            for file_id in files {
                // FIXME: handle error.
                self.remove_file(file_id).await?;
                self.reader_cache.invalidate(file_id);
            }
            Ok(())
        }

        async fn remove_file(&self, file_id: u32) -> Result<()> {
            let path = self.base.join(format!("{}_{file_id}", FILE_PREFIX));
            self.env
                .remove_file(&path)
                .await
                .expect("remove page file failed");
            Ok(())
        }

        pub(crate) fn populate_cache(&self, page_addr: u64, page_content: &[u8]) -> Result<()> {
            if !self.prepopulate_cache_on_flush {
                return Ok(());
            }
            let val = page_content.to_owned(); // TODO: aligned buffer pool
            let guard = match self.page_cache.insert(
                page_addr,
                Some(val),
                page_content.len(),
                CacheOption::default(),
            ) {
                Ok(guard) => guard,
                Err(Error::MemoryLimit) => return Ok(()),
                Err(err) => return Err(err),
            };
            drop(guard);
            Ok(())
        }

        pub(crate) fn evict_cached_pages(&self, page_addrs: &[u64]) {
            for page_addr in page_addrs {
                self.page_cache.erase(*page_addr);
            }
        }

        pub(crate) fn list_files(&self) -> Result<Vec<u32>> {
            let prefix = format!("{}_", FILE_PREFIX).into_bytes();
            self.list_files_with_prefix(&prefix)
        }

        fn list_files_with_prefix(&self, prefix: &[u8]) -> Result<Vec<u32>> {
            use std::os::unix::ffi::OsStrExt;

            let dir = self.env.read_dir(&self.base)?;
            let mut files = Vec::default();
            for entry in dir {
                let file_name = entry?.file_name();
                let bytes = file_name.as_bytes();
                if !bytes.starts_with(prefix) {
                    continue;
                }
                if let Ok(file_id) = String::from_utf8_lossy(&bytes[prefix.len()..]).parse::<u32>()
                {
                    files.push(file_id);
                }
            }

            Ok(files)
        }

        pub(crate) fn stats(&self) -> (CacheStats, CacheStats) {
            let page_cache = self.page_cache.stats();
            let table_cache = self.reader_cache.stats();
            (page_cache, table_cache)
        }
    }

    #[cfg(test)]
    mod tests {
        use tempdir::TempDir;

        use super::*;
        use crate::page::PageInfo;

        #[photonio::test]
        fn test_file_builder() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_builder").unwrap();
            let files = PageFiles::new(env, base.path(), &test_option()).await;
            let builder = files
                .new_file_builder(11233, Compression::ZSTD, ChecksumType::NONE)
                .await
                .unwrap();
            let mut builder = builder.add_page_group(123);
            builder.add_dealloc_pages(&[1, 2]);
            builder
                .add_page(3, 1, empty_page_info(), &[3, 4, 1])
                .await
                .unwrap();
            let builder = builder.finish().await.unwrap();
            builder.finish(0).await.unwrap();
        }

        #[photonio::test]
        fn test_read_page() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_dread").unwrap();
            let mut opt = test_option();
            opt.page_checksum_type = ChecksumType::NONE;
            let files = PageFiles::new(env, base.path(), &opt).await;
            let file_id = 2;
            let (group, info) = {
                let b = files
                    .new_file_builder(2, Compression::NONE, ChecksumType::NONE)
                    .await
                    .unwrap();
                let mut b = b.add_page_group(123);
                b.add_dealloc_pages(&[page_addr(1, 0), page_addr(1, 1)]);
                b.add_page(1, page_addr(2, 2), empty_page_info(), &[7].repeat(8192))
                    .await
                    .unwrap();
                b.add_page(2, page_addr(2, 3), empty_page_info(), &[8].repeat(8192 / 2))
                    .await
                    .unwrap();
                b.add_page(3, page_addr(2, 4), empty_page_info(), &[9].repeat(8192 / 3))
                    .await
                    .unwrap();
                let builder = b.finish().await.unwrap();
                let (groups, info) = builder.finish(1).await.unwrap();
                assert!(info.meta().file_size > 8192 + 8192 / 2 + 8192 / 3);
                (groups.get(&123).unwrap().clone(), info)
            };

            {
                // read aligned 1st page.
                let hd = group.get_page_handle(page_addr(2, 2)).unwrap();
                files
                    .read_file_page(file_id, info.meta(), hd)
                    .await
                    .unwrap();
            }

            {
                // read unaligned(need trim end) 2nd page.
                let hd = group.get_page_handle(page_addr(2, 3)).unwrap();
                files
                    .read_file_page(file_id, info.meta(), hd)
                    .await
                    .unwrap();
            }

            {
                // read unaligned(need trim both start and end) 3rd page.
                let hd = group.get_page_handle(page_addr(2, 3)).unwrap();
                files
                    .read_file_page(file_id, info.meta(), hd)
                    .await
                    .unwrap();
            }
        }

        #[photonio::test]
        fn test_simple_write_reader() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_simple_rw").unwrap();
            let files = PageFiles::new(env, base.path(), &test_option()).await;

            let file_id = 2;
            {
                let b = files
                    .new_file_builder(file_id, Compression::SNAPPY, ChecksumType::NONE)
                    .await
                    .unwrap();
                let mut b = b.add_page_group(1);
                b.add_dealloc_pages(&[page_addr(1, 0), page_addr(1, 1)]);
                b.add_page(1, page_addr(2, 2), empty_page_info(), &[7].repeat(8192))
                    .await
                    .unwrap();
                b.add_page(2, page_addr(2, 3), empty_page_info(), &[8].repeat(8192 / 2))
                    .await
                    .unwrap();
                b.add_page(3, page_addr(2, 4), empty_page_info(), &[9].repeat(8192 / 3))
                    .await
                    .unwrap();
                let b = b.finish().await.unwrap();
                b.finish(1).await.unwrap();
            };
            {
                let meta = files.read_file_meta(file_id).await.unwrap();
                let group = meta.page_groups.get(&1).unwrap();
                {
                    let (_, handle) = group.get_page_handle(page_addr(2, 4)).unwrap();
                    let buf = files
                        .read_file_page(file_id, &meta.file_meta, handle)
                        .await
                        .unwrap();
                    assert_eq!(buf.as_slice(), &[9].repeat(buf.len()));
                }
            }
        }

        #[photonio::test]
        fn test_query_page_id_by_addr() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_query_id_by_addr").unwrap();
            let files = PageFiles::new(env, base.path(), &test_option()).await;
            let file_id = 1;
            let page_addr1 = page_addr(file_id, 0);
            let page_addr2 = page_addr(file_id, 1);
            let page_addr3 = page_addr(file_id, 2);

            {
                let b = files
                    .new_file_builder(file_id, Compression::ZSTD, ChecksumType::NONE)
                    .await
                    .unwrap();
                let mut b = b.add_page_group(1);
                b.add_page(1, page_addr1, empty_page_info(), &[1].repeat(10))
                    .await
                    .unwrap();
                b.add_page(1, page_addr2, empty_page_info(), &[2].repeat(10))
                    .await
                    .unwrap();
                b.add_page(2, page_addr3, empty_page_info(), &[3].repeat(10))
                    .await
                    .unwrap();
                let b = b.finish().await.unwrap();
                let (groups, _) = b.finish(1).await.unwrap();
                let group = groups.get(&1).unwrap();
                assert!(group.get_page_handle(page_addr1).is_some())
            }

            {
                let meta = files.read_file_meta(file_id).await.unwrap();
                let page_table = meta.page_tables.get(&1).unwrap();
                assert_eq!(*page_table.get(&page_addr1).unwrap(), 1);
                assert_eq!(*page_table.get(&page_addr2).unwrap(), 1);
                assert_eq!(*page_table.get(&page_addr3).unwrap(), 2);
            }
        }

        #[photonio::test]
        fn test_get_child_page() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_get_child_page").unwrap();
            let files = PageFiles::new(env, base.path(), &test_option()).await;

            let file_id = 1;
            let page_addr1 = page_addr(file_id, 0);
            let page_addr2 = page_addr(file_id, 1);

            {
                let b = files
                    .new_file_builder(file_id, Compression::ZSTD, ChecksumType::NONE)
                    .await
                    .unwrap();
                let mut b = b.add_page_group(1);
                b.add_page(1, page_addr1, empty_page_info(), &[1].repeat(10))
                    .await
                    .unwrap();
                b.add_page(1, page_addr2, empty_page_info(), &[2].repeat(10))
                    .await
                    .unwrap();
                let b = b.finish().await.unwrap();
                let (groups, _) = b.finish(1).await.unwrap();
                let group = groups.get(&1).unwrap();
                assert!(group.get_page_handle(page_addr1).is_some())
            }
        }

        #[photonio::test]
        async fn test_list_page_files() {
            async fn new_file(files: &PageFiles<crate::env::Photon>, file_id: u32) {
                let b = files
                    .new_file_builder(file_id, Compression::ZSTD, ChecksumType::NONE)
                    .await
                    .unwrap();
                let b = b.add_page_group(file_id);
                b.finish().await.unwrap().finish(1).await.unwrap();
            }

            let env = crate::env::Photon;
            let base = TempDir::new("test_get_child_page").unwrap();
            let files = PageFiles::new(env, base.path(), &test_option()).await;
            new_file(&files, 0).await;
            new_file(&files, 1).await;
            new_file(&files, 3).await;
            new_file(&files, 5).await;
            new_file(&files, 7).await;
            new_file(&files, 9).await;
            new_file(&files, 123321).await;
            new_file(&files, u32::MAX).await;
            let mut files = files.list_files().unwrap();
            files.sort_unstable();
            assert_eq!(files, vec![0, 1, 3, 5, 7, 9, 123321, u32::MAX]);
        }

        fn page_addr(file_id: u32, index: u32) -> u64 {
            ((file_id as u64) << 32) | (index as u64)
        }

        fn test_option() -> PageStoreOptions {
            PageStoreOptions {
                cache_capacity: 2 << 10,
                ..Default::default()
            }
        }

        fn empty_page_info() -> PageInfo {
            PageInfo::from_raw(0, 0, 0)
        }
    }
}
