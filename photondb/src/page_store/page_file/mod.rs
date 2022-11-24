mod file_builder;
pub(crate) use file_builder::FileBuilder;
pub(self) use file_builder::{BlockHandler, BufferedWriter, CommonFileBuilder};

mod file_reader;
pub(crate) use file_reader::{read_page_table, PageFileReader};

mod info_builder;
pub(crate) use info_builder::FileInfoBuilder;

mod types;
pub(crate) use facade::PageFiles;
pub(crate) use types::{FileInfo, FileMeta, MapFileInfo};

mod map_file_builder;
pub(crate) use map_file_builder::{MapFileBuilder, PartialFileBuilder};

mod map_file_reader;
pub(crate) use map_file_reader::{MapFileMetaReader, MapFileReader};

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
        *,
    };
    use crate::{
        env::{Env, PositionalReader, SequentialWriter},
        page_store::Result,
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

        reader_cache: file_reader::ReaderCache<E>,
    }

    impl<E: Env> PageFiles<E> {
        /// Create page file facade.
        /// It should be a singleton in the page_store.
        pub(crate) async fn new(env: E, base: impl Into<PathBuf>, use_direct: bool) -> Self {
            let base = base.into();
            let base_dir = env.open_dir(&base).await.expect("open base dir fail");
            let reader_cache = ReaderCache::new(MAX_OPEN_READER_FD_NUM);
            Self {
                env,
                base,
                base_dir,
                use_direct,
                reader_cache,
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

        // Create info_builder to help recovery & mantains version's file_info.
        pub(crate) fn new_info_builder(&self) -> FileInfoBuilder<E> {
            FileInfoBuilder::new(self.env.to_owned(), self.base.to_owned())
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

        #[allow(unused)]
        pub(crate) async fn open_map_file_meta_reader(
            &self,
            file_id: u32,
        ) -> Result<MapFileMetaReader<<E as Env>::PositionalReader>> {
            let (file, file_size) = self
                .open_positional_reader(PAGE_FILE_PREFIX, file_id)
                .await?;
            let page_file_reader = Arc::new(MapFileReader::from(
                file,
                true,
                DEFAULT_BLOCK_SIZE,
                file_size as usize,
            ));
            MapFileMetaReader::open(file_id, page_file_reader).await
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
        use std::collections::HashMap;

        use tempdir::TempDir;

        use super::*;

        #[photonio::test]
        fn test_file_builder() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_builder").unwrap();
            let files = PageFiles::new(env, base.path(), true).await;
            let mut builder = files.new_page_file_builder(11233).await.unwrap();
            builder.add_delete_pages(&[1, 2]);
            builder.add_page(3, 1, &[3, 4, 1]).await.unwrap();
            builder.finish().await.unwrap();
        }

        #[photonio::test]
        fn test_read_page() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_dread").unwrap();
            let files = PageFiles::new(env, base.path(), true).await;
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
            let files = PageFiles::new(env, base.path(), true).await;

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
        fn test_file_info_recovery_and_add_new_file() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_recovery").unwrap();
            let files = PageFiles::new(env, base.path(), true).await;

            let info_builder = files.new_info_builder();
            {
                // test add new files.
                let mut mock_version = HashMap::new();
                {
                    let file_id = 1;
                    let mut b = files.new_page_file_builder(file_id).await.unwrap();
                    b.add_page(1, page_addr(file_id, 0), &[1].repeat(10))
                        .await
                        .unwrap();
                    b.add_page(2, page_addr(file_id, 1), &[2].repeat(10))
                        .await
                        .unwrap();
                    b.add_page(3, page_addr(file_id, 2), &[3].repeat(10))
                        .await
                        .unwrap();
                    let file_info = b.finish().await.unwrap();
                    mock_version.insert(file_id, file_info);
                }

                {
                    // add an additional file with delete file1's page info.
                    let file_id = 2;
                    let delete_pages = &[page_addr(1, 0)];

                    let mut b = files.new_page_file_builder(file_id).await.unwrap();
                    b.add_page(4, page_addr(file_id, 0), &[1].repeat(10))
                        .await
                        .unwrap();
                    b.add_page(5, page_addr(file_id, 1), &[2].repeat(10))
                        .await
                        .unwrap();
                    b.add_page(6, page_addr(file_id, 4), &[3].repeat(10))
                        .await
                        .unwrap();
                    b.add_delete_pages(delete_pages);
                    let file_info = b.finish().await.unwrap();

                    let original_file1_active_size = mock_version.get(&1).unwrap().effective_size();
                    mock_version = info_builder
                        .add_file_info(&mock_version, file_info, delete_pages)
                        .unwrap();

                    let file1 = mock_version.get(&1).unwrap();
                    assert_eq!(file1.effective_size(), original_file1_active_size - 10);
                    assert!(file1.get_page_handle(page_addr(1, 0)).is_none());
                    assert!(file1.get_page_handle(page_addr(1, 1)).is_some());

                    let file2 = mock_version.get(&2).unwrap();
                    let hd = file2.get_page_handle(page_addr(2, 4)).unwrap();
                    assert_eq!(hd.size, 10);
                    assert_eq!(hd.offset, 20);
                }

                {
                    let file_id = 3;
                    let delete_pages = &[page_addr(2, 0)];
                    let mut b = files.new_page_file_builder(file_id).await.unwrap();
                    b.add_delete_pages(delete_pages);
                    let file_info = b.finish().await.unwrap();
                    info_builder
                        .add_file_info(&mock_version, file_info, delete_pages)
                        .unwrap();
                }
            }

            {
                // test recovery file_info from folder.
                let known_files = &[1, 2, 3]
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect::<Vec<_>>();
                let recovery_mock_version = info_builder
                    .recover_page_file_infos(known_files)
                    .await
                    .unwrap();
                let file1 = recovery_mock_version.get(&1).unwrap();
                assert_eq!(file1.effective_size(), 20);
                assert!(file1.get_page_handle(page_addr(1, 0)).is_none());
                let file2 = recovery_mock_version.get(&2).unwrap();
                assert_eq!(file2.effective_size(), 20);
                let file2 = recovery_mock_version.get(&2).unwrap();
                let hd = file2.get_page_handle(page_addr(2, 4)).unwrap();
                assert_eq!(hd.size, 10);
                assert_eq!(hd.offset, 20);

                let meta_reader = files.open_page_file_meta_reader(3).await.unwrap();
                let page_table = meta_reader.read_page_table().await.unwrap();
                assert!(page_table.is_empty())
            }
        }

        #[photonio::test]
        fn test_query_page_id_by_addr() {
            let env = crate::env::Photon;
            let base = TempDir::new("test_query_id_by_addr").unwrap();
            let files = PageFiles::new(env, base.path(), true).await;
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
            let files = PageFiles::new(env, base.path(), true).await;
            let info_builder = files.new_info_builder();

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

            {
                let recovery_mock_version = info_builder
                    .recover_page_file_infos(&[file_id.into()])
                    .await
                    .unwrap();
                let file1 = recovery_mock_version.get(&1).unwrap();
                assert!(file1.get_page_handle(page_addr1).is_some())
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
            let files = PageFiles::new(env, base.path(), true).await;
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
    }
}
