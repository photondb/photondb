mod file_builder;
pub(crate) use file_builder::FileBuilder;
pub(self) use file_builder::{BlockHandler, BufferedWriter, CommonFileBuilder};

mod file_reader;
pub(crate) use file_reader::PageFileReader;

mod types;
pub(crate) use facade::PageFiles;
pub(crate) use types::{FileInfo, FileMeta, MapFileInfo};

mod map_file_builder;
pub(crate) use map_file_builder::{MapFileBuilder, PartialFileBuilder};

mod map_file_reader;
pub(crate) use map_file_reader::{MapFileMetaHolder, MapFileReader};

pub(crate) mod constant {
    /// Default alignment requirement for the SSD.
    // TODO: query logical sector size
    // like: https://github.com/DataDog/glommio/issues/7 or https://github.com/facebook/rocksdb/pull/1875
    pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4096;

    pub(crate) const IO_BUFFER_SIZE: usize = 4096 * 4;

    pub(crate) const MAX_OPEN_READER_FD_NUM: u64 = 1000;

    pub(crate) const PAGE_FILE_MAGIC: u64 = 142857;
    pub(crate) const MAP_FILE_MAGIC: u64 = 0x179394;
}

pub(crate) mod facade {
    use std::{path::PathBuf, sync::Arc};

    use super::{
        constant::{DEFAULT_BLOCK_SIZE, MAX_OPEN_READER_FD_NUM},
        file_reader::{self, MetaReader, ReaderCache},
        types::PageHandle,
        *,
    };
    use crate::{
        env::{Env, PositionalReader, SequentialWriter},
        page_store::{Cache, CacheEntry, ClockCache, Result},
        PageStoreOptions,
    };

    pub(crate) const PAGE_FILE_PREFIX: &str = "dat";
    pub(crate) const MAP_FILE_PREFIX: &str = "map";

    /// The facade for page_file module.
    /// it hides the detail about disk location for caller(after it be created).
    pub(crate) struct PageFiles<E: Env> {
        env: E,
        base: PathBuf,
        base_dir: E::Directory,

        use_direct: bool,
        prepopulate_cache_on_flush: bool,

        reader_cache: file_reader::ReaderCache<E>,
        page_cache: Arc<ClockCache<Vec<u8>>>,
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
            let reader_cache = ReaderCache::new(MAX_OPEN_READER_FD_NUM);
            let page_cache = Arc::new(ClockCache::new(
                options.cache_capacity,
                options.cache_estimated_entry_charge,
                -1,
                false,
            ));
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

        /// Create `FileBuilder` to write a new page file.
        pub(crate) async fn new_page_file_builder(&self, file_id: u32) -> Result<FileBuilder<E>> {
            // TODO: switch to env in suitable time.
            let path = self.base.join(format!("{}_{file_id}", PAGE_FILE_PREFIX));
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
            ))
        }

        /// Create `MapFileBuilder` to write a new map file.
        pub(crate) async fn new_map_file_builder(&self, file_id: u32) -> Result<MapFileBuilder<E>> {
            // TODO: switch to env in suitable time.
            let path = self.base.join(format!("{}_{file_id}", MAP_FILE_PREFIX));
            let writer = self
                .env
                .open_sequential_writer(path.to_owned())
                .await
                .expect("open writer for file_id: {file_id} fail");
            let use_direct = self.use_direct && writer.direct_io_ify().is_ok();
            Ok(MapFileBuilder::new(
                file_id,
                &self.base_dir,
                writer,
                use_direct,
                DEFAULT_BLOCK_SIZE,
            ))
        }

        pub(crate) async fn read_page(
            &self,
            file_id: u32,
            addr: u64,
            handle: PageHandle,
            file_info: &FileInfo,
        ) -> Result<CacheEntry<Vec<u8>, ClockCache<Vec<u8>>>> {
            if let Some(cache_entry) = self.page_cache.lookup(addr) {
                return Ok(cache_entry);
            }

            let reader = self
                .open_page_reader(file_id, file_info.meta().block_size())
                .await?;

            let mut buf = vec![0u8; handle.size as usize]; // TODO: aligned buffer pool
            reader.read_exact_at(&mut buf, handle.offset as u64).await?;

            let charge = buf.len();
            let cache_entry = self
                .page_cache
                .insert(addr, Some(buf), charge)
                .expect("insert cache fail");

            Ok(cache_entry.unwrap())
        }

        /// Open page_reader for a page_file.
        /// page_store could get file_id & block_size from page_addr's high bit
        /// and version.active_files.
        pub(crate) async fn open_page_reader(
            &self,
            file_id: u32,
            block_size: usize,
        ) -> Result<Arc<PageFileReader<E::PositionalReader>>> {
            let r = self
                .reader_cache
                .get_with(file_id, async move {
                    let (file, file_size) = self
                        .open_positional_reader(PAGE_FILE_PREFIX, file_id)
                        .await
                        .expect("open reader for file_id: {file_id} fail");
                    let use_direct = self.use_direct && file.direct_io_ify().is_ok();
                    Arc::new(PageFileReader::from(
                        file,
                        use_direct,
                        block_size,
                        file_size as usize,
                    ))
                })
                .await;
            Ok(r)
        }

        /// Open a reader for the specified map file.
        #[allow(unused)]
        pub(crate) async fn open_map_file_reader(
            &self,
            file_id: u32,
            block_size: usize,
        ) -> Result<Arc<MapFileReader<E::PositionalReader>>> {
            todo!("support file cache")
        }

        pub(crate) async fn open_page_file_meta_reader(
            &self,
            file_id: u32,
        ) -> Result<MetaReader<<E as Env>::PositionalReader>> {
            let (file, file_size) = self
                .open_positional_reader(PAGE_FILE_PREFIX, file_id)
                .await?;
            let page_file_reader = Arc::new(PageFileReader::from(
                file,
                true,
                DEFAULT_BLOCK_SIZE,
                file_size as usize,
            ));
            MetaReader::open(page_file_reader, file_id).await
        }

        pub(crate) async fn read_map_file_meta(&self, file_id: u32) -> Result<MapFileMetaHolder> {
            let (file, file_size) = self
                .open_positional_reader(PAGE_FILE_PREFIX, file_id)
                .await?;
            let page_file_reader = Arc::new(MapFileReader::from(
                file,
                true,
                DEFAULT_BLOCK_SIZE,
                file_size as usize,
            ));
            MapFileMetaHolder::read(file_id, page_file_reader).await
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

        pub(crate) async fn remove_page_files(&self, files: Vec<u32>) -> Result<()> {
            for file_id in files {
                // FIXME: handle error.
                self.remove_page_file(file_id).await?;
                self.reader_cache.invalidate(&file_id).await;
            }
            Ok(())
        }

        pub(crate) async fn remove_map_files(&self, files: Vec<u32>) -> Result<()> {
            for file_id in files {
                // FIXME: handle error.
                self.remove_map_file(file_id).await?;
            }
            Ok(())
        }

        async fn remove_page_file(&self, file_id: u32) -> Result<()> {
            let path = self.base.join(format!("{}_{file_id}", PAGE_FILE_PREFIX));
            self.env
                .remove_file(&path)
                .await
                .expect("remove page file failed");
            Ok(())
        }

        async fn remove_map_file(&self, file_id: u32) -> Result<()> {
            let path = self.base.join(format!("{}_{file_id}", MAP_FILE_PREFIX));
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
            let guard = self
                .page_cache
                .insert(page_addr, Some(val), page_content.len())?;
            drop(guard);
            Ok(())
        }

        pub(crate) fn evict_cached_pages(&self, page_addrs: &[u64]) {
            for page_addr in page_addrs {
                self.page_cache.erase(*page_addr);
            }
        }

        pub(crate) fn list_page_files(&self) -> Result<Vec<u32>> {
            let prefix = format!("{}_", PAGE_FILE_PREFIX).into_bytes();
            self.list_files_with_prefix(&prefix)
        }

        pub(crate) fn list_map_files(&self) -> Result<Vec<u32>> {
            let prefix = format!("{}_", MAP_FILE_PREFIX).into_bytes();
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
    }

    #[cfg(test)]
    mod tests {
        use tempdir::TempDir;

        use super::*;

        #[photonio::test]
        fn test_file_builder() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_builder").unwrap();
            let files = PageFiles::new(env, base.path(), &test_option()).await;
            let mut builder = files.new_page_file_builder(11233).await.unwrap();
            builder.add_delete_pages(&[1, 2]);
            builder.add_page(3, 1, &[3, 4, 1]).await.unwrap();
            builder.finish().await.unwrap();
        }

        #[photonio::test]
        fn test_read_page() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_dread").unwrap();
            let files = PageFiles::new(env, base.path(), &test_option()).await;
            let file_id = 2;
            let info = {
                let mut b = files.new_page_file_builder(file_id).await.unwrap();
                b.add_delete_pages(&[page_addr(1, 0), page_addr(1, 1)]);
                b.add_page(1, page_addr(2, 2), &[7].repeat(8192))
                    .await
                    .unwrap();
                b.add_page(2, page_addr(2, 3), &[8].repeat(8192 / 2))
                    .await
                    .unwrap();
                b.add_page(3, page_addr(2, 4), &[9].repeat(8192 / 3))
                    .await
                    .unwrap();
                let info = b.finish().await.unwrap();
                assert_eq!(info.effective_size(), 8192 + 8192 / 2 + 8192 / 3);
                info
            };

            let page_reader = files
                .open_page_reader(info.meta().get_file_id(), 4096)
                .await
                .unwrap();

            {
                // read aligned 1st page.
                let hd = info.get_page_handle(page_addr(2, 2)).unwrap();
                let mut buf = vec![0u8; hd.size as usize];
                page_reader
                    .read_exact_at(&mut buf, hd.offset as u64)
                    .await
                    .unwrap();
            }

            {
                // read unaligned(need trim end) 2nd page.
                let hd = info.get_page_handle(page_addr(2, 3)).unwrap();
                let mut buf = vec![0u8; hd.size as usize];
                page_reader
                    .read_exact_at(&mut buf, hd.offset as u64)
                    .await
                    .unwrap();
            }

            {
                // read unaligned(need trim both start and end) 3rd page.
                let hd = info.get_page_handle(page_addr(2, 3)).unwrap();
                let mut buf = vec![0u8; hd.size as usize];
                page_reader
                    .read_exact_at(&mut buf, hd.offset as u64)
                    .await
                    .unwrap();
            }
        }

        #[photonio::test]
        fn test_test_simple_write_reader() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_simple_rw").unwrap();
            let files = PageFiles::new(env, base.path(), &test_option()).await;

            let file_id = 2;
            let ret_info = {
                let mut b = files.new_page_file_builder(file_id).await.unwrap();
                b.add_delete_pages(&[page_addr(1, 0), page_addr(1, 1)]);
                b.add_page(1, page_addr(2, 2), &[7].repeat(8192))
                    .await
                    .unwrap();
                b.add_page(2, page_addr(2, 3), &[8].repeat(8192 / 2))
                    .await
                    .unwrap();
                b.add_page(3, page_addr(2, 4), &[9].repeat(8192 / 3))
                    .await
                    .unwrap();
                let info = b.finish().await.unwrap();
                assert_eq!(info.effective_size(), 8192 + 8192 / 2 + 8192 / 3);
                info
            };
            {
                let meta = {
                    let meta_reader = files.open_page_file_meta_reader(file_id).await.unwrap();
                    assert_eq!(
                        meta_reader.file_metadata().total_page_size(),
                        8192 + 8192 / 2 + 8192 / 3
                    );
                    let page3 = page_addr(2, 4);
                    let (page3_offset, page3_size) =
                        meta_reader.file_metadata().get_page_handle(page3).unwrap();
                    let handle = ret_info.get_page_handle(page3).unwrap();
                    assert_eq!(page3_offset as u32, handle.offset);
                    assert_eq!(page3_size, 8192 / 3);

                    let page_table = meta_reader.read_page_table().await.unwrap();
                    assert_eq!(page_table.len(), 3);

                    let delete_tables = meta_reader.read_delete_pages().await.unwrap();
                    assert_eq!(delete_tables.len(), 2);
                    meta_reader.file_metadata()
                };

                {
                    let (page3_offset, page3_size) = meta.get_page_handle(page_addr(2, 4)).unwrap();
                    let page_reader = files.open_page_reader(file_id, 4096).await.unwrap();
                    let mut buf = vec![0u8; page3_size];
                    page_reader
                        .read_exact_at(&mut buf, page3_offset)
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
                let mut b = files.new_page_file_builder(file_id).await.unwrap();
                b.add_page(1, page_addr1, &[1].repeat(10)).await.unwrap();
                b.add_page(1, page_addr2, &[2].repeat(10)).await.unwrap();
                b.add_page(2, page_addr3, &[3].repeat(10)).await.unwrap();
                let file_info = b.finish().await.unwrap();
                assert!(file_info.get_page_handle(page_addr1).is_some())
            }

            {
                let meta_reader = files.open_page_file_meta_reader(file_id).await.unwrap();
                let page_table = meta_reader.read_page_table().await.unwrap();
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
                let mut b = files.new_page_file_builder(file_id).await.unwrap();
                b.add_page(1, page_addr1, &[1].repeat(10)).await.unwrap();
                b.add_page(1, page_addr2, &[2].repeat(10)).await.unwrap();
                let file_info = b.finish().await.unwrap();
                assert!(file_info.get_page_handle(page_addr1).is_some())
            }
        }

        #[photonio::test]
        async fn test_list_page_files() {
            async fn new_file(files: &PageFiles<crate::env::Photon>, file_id: u32) {
                let mut b = files.new_page_file_builder(file_id).await.unwrap();
                b.finish().await.unwrap();
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
            let mut files = files.list_page_files().unwrap();
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
    }
}
