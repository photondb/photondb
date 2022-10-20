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
            Ok(FileBuilder::new(writer, self.use_direct))
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
                .expect("expect file_id: {file_id}'s file already exist");
            FileReader::open_with_meta(reader, file_meta)
        }

        // Create info_builder to help recovery & mantains version's file_info.
        pub(crate) fn new_info_builder(&self) -> FileInfoBuilder {
            FileInfoBuilder::new(self.base.to_owned(), &self.file_prefix)
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
    }
}
