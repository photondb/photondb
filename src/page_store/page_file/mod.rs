mod file_builder;
pub(crate) use file_builder::FileBuilder;

mod file_reader;
pub(crate) use file_reader::FileReader;

mod info_builder;
pub(crate) use info_builder::FileInfoBuilder;

mod types;
pub(crate) use types::{FileInfo, FileMeta, PageHandle};

/// [`FileInfoIterator`] is used to traverse [`FileInfo`] to get the
/// [`PageHandle`] of all active pages.
pub(crate) struct FileInfoIterator<'a> {
    info: &'a FileInfo,
    index: u32,
}

impl<'a> Iterator for FileInfoIterator<'a> {
    type Item = PageHandle;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub(crate) mod facade {
    use std::{path::PathBuf, sync::Arc};

    use photonio::fs::{File, OpenOptions};

    use super::*;
    use crate::page_store::Result;

    /// The facade for page_file module.
    /// it hides the detail about disk location for caller(after it be created).
    pub(crate) struct PageFiles {
        base: PathBuf,

        file_prefix: String,
        use_direct: bool,
    }

    impl PageFiles {
        /// Create page file facade.
        /// It should be a singleton in the page_store.
        pub(crate) fn new(base: impl Into<PathBuf>, file_prefile: &str) -> Self {
            Self {
                base: base.into(),
                file_prefix: file_prefile.into(),
                use_direct: true,
            }
        }

        /// Create file_builder to write a new page_file.
        pub(crate) async fn new_file_builder(&self, file_id: u32) -> Result<FileBuilder> {
            // TODO: switch to env in suitable time.
            use std::os::unix::prelude::OpenOptionsExt;
            let path = self.base.join(format!("{}_{file_id}", self.file_prefix));
            let flags = self.writer_flags();
            let writer = OpenOptions::new()
                .write(true)
                .custom_flags(flags)
                .create(true)
                .truncate(true)
                .open(path)
                .await
                .expect("open file_id: {file_id}'s file fail");
            Ok(FileBuilder::new(file_id, writer, self.use_direct))
        }

        #[inline]
        fn writer_flags(&self) -> i32 {
            const O_DIRECT_LINUX: i32 = 0x4000;
            const O_DIRECT_AARCH64: i32 = 0x10000;
            if !self.use_direct {
                return 0;
            }
            if cfg!(not(target_os = "linux")) {
                0
            } else if cfg!(target_arch = "aarch64") {
                O_DIRECT_AARCH64
            } else {
                O_DIRECT_LINUX
            }
        }

        /// Open file_reader for a page_file.
        /// page_store should get file_meta from current version with page_addr
        /// before call this method.
        pub(crate) async fn open_file_reader(
            &self,
            file_meta: Arc<FileMeta>,
        ) -> Result<FileReader<File>> {
            let path = self
                .base
                .join(format!("{}_{}", self.file_prefix, file_meta.get_file_id()));
            let reader = File::open(path)
                .await
                .expect("open reader for file_id: {file_id} fail");
            FileReader::open_with_meta(reader, file_meta)
        }

        // Create info_builder to help recovery & mantains version's file_info.
        pub(crate) fn new_info_builder(&self) -> FileInfoBuilder {
            FileInfoBuilder::new(self.base.to_owned(), &self.file_prefix)
        }

        pub(self) async fn open_file_meta(&self, file_id: u32) -> Result<Arc<FileMeta>> {
            let path = self.base.join(format!("{}_{}", self.file_prefix, file_id));
            let reader = File::open(path)
                .await
                .expect("open reader for file_id: {file_id} fail");
            let file_size = reader
                .metadata()
                .await
                .expect("read fs metadata fail")
                .len();
            FileReader::open_file_meta(&reader, file_size as u32, file_id).await
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[photonio::test]
        fn test_file_builder() {
            let base = std::env::temp_dir();
            let files = PageFiles::new(&base, "testdata");
            let mut builder = files.new_file_builder(11233).await.unwrap();
            builder.add_delete_pages(&[1, 2]);
            builder.add_page(3, 1, &[3, 4, 1]).await.unwrap();
            builder.finish().await.unwrap();
        }

        #[photonio::test]
        fn test_test_simple_write_reader() {
            let files = {
                let base = std::env::temp_dir();
                PageFiles::new(&base, "testdata")
            };

            let file_id = 2;
            let ret_info = {
                let mut b = files.new_file_builder(file_id).await.unwrap();
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
                let file_meta = files.open_file_meta(file_id).await.unwrap(); // normally get from current version, no need reopen.
                assert_eq!(file_meta.total_page_size(), 8192 + 8192 / 2 + 8192 / 3);

                let reader = files.open_file_reader(file_meta.to_owned()).await.unwrap();
                let page3 = page_addr(2, 4);
                let (page3_offset, page3_size) = file_meta.get_page_handle(page3).unwrap();
                let handle = ret_info.get_page_handle(page3).unwrap();
                assert_eq!(page3_offset as u32, handle.offset);
                assert_eq!(page3_size, 8192 / 3);
                let mut buf = vec![0u8; page3_size];
                reader.read_page(page3, &mut buf).await.unwrap();
                assert_eq!(buf.as_slice(), &[9].repeat(buf.len()));

                let page_table = reader.read_page_table().await.unwrap();
                assert_eq!(page_table.len(), 3);

                let delete_tables = reader.read_delete_pages().await.unwrap();
                assert_eq!(delete_tables.len(), 2);
            }
        }

        fn page_addr(file_id: u32, index: u32) -> u64 {
            ((file_id as u64) << 32) | (index as u64)
        }
    }
}
