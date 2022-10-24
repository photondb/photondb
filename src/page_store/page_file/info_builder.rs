use std::{collections::HashMap, path::PathBuf};

use photonio::fs::File;

use super::{
    file_builder::logical_block_size, file_reader::MetaReader, types::split_page_addr, FileInfo,
    PageFileReader,
};
use crate::page_store::Result;

pub(crate) struct FileInfoBuilder {
    base: PathBuf,
    file_prefix: String,
}

impl FileInfoBuilder {
    /// Create file info builder.
    /// it need base dir and page file name prefix.
    pub(crate) fn new(base: impl Into<PathBuf>, file_prefix: &str) -> Self {
        Self {
            base: base.into(),
            file_prefix: file_prefix.into(),
        }
    }

    /// Recovery file infos for specified file ids.
    /// It could be used to recovery in-memory `Version`'s `active_files` after
    /// it be recoveried from manifest files.
    pub(crate) async fn recovery_base_file_infos(
        &self,
        file_ids: &[u32],
    ) -> Result<HashMap<u32, FileInfo>> {
        let mut files = HashMap::with_capacity(file_ids.len());
        let mut delete_pages = Vec::new();
        for file_id in file_ids {
            let (info, file_delete_pages) = self.recovery_one_file(file_id).await?;
            files.insert(*file_id, info);
            delete_pages.extend_from_slice(&file_delete_pages);
        }
        Self::mantain_file_active_pages(&mut files, &delete_pages);
        Ok(files)
    }

    // Add new file to exist file infos.
    // It copies the old file infos before add new file.
    // It could help version to prepare new Version's `active_files` after flush a
    // new page file.
    pub(crate) fn add_file_info(
        &self,
        files: &HashMap<u32, FileInfo>,
        new_file_info: FileInfo,
        new_delete_pages: &[u64],
    ) -> Result<HashMap<u32, FileInfo>> {
        let mut files = files.clone();
        files.insert(new_file_info.get_file_id(), new_file_info);
        Self::mantain_file_active_pages(&mut files, new_delete_pages);
        Ok(files)
    }

    async fn recovery_one_file(&self, file_id: &u32) -> Result<(FileInfo, Vec<u64>)> {
        let meta_reader = self.open_meta_reader(file_id).await;
        let meta = meta_reader.file_metadata();

        let active_pages = {
            let page_table = meta_reader
                .read_page_table()
                .await
                .expect("read page table error");
            let mut active_pages = roaring::RoaringBitmap::new();
            for (_page_id, page_addr) in page_table {
                let (_, index) = split_page_addr(page_addr);
                active_pages.insert(index);
            }
            active_pages
        };

        let active_size = meta.total_page_size();

        let delete_pages = meta_reader.read_delete_pages().await?;

        let decline_rate = 0.0; // todo:...
        let info = FileInfo::new(active_pages, decline_rate, active_size, meta);

        Ok((info, delete_pages))
    }

    #[inline]
    fn mantain_file_active_pages(files: &mut HashMap<u32, FileInfo>, delete_pages: &[u64]) {
        for page_addr in delete_pages {
            let (file_id, _) = split_page_addr(*page_addr);
            if let Some(info) = files.get_mut(&file_id) {
                info.deactivate_page(*page_addr)
            }
        }
    }

    #[inline]
    async fn open_meta_reader(&self, file_id: &u32) -> MetaReader<File> {
        let path = self.base.join(format!("{}_{file_id}", self.file_prefix));
        let raw_file = File::open(&path).await.expect("file {path} not exist");
        let raw_metadata = raw_file.metadata().await.expect("read file metadata file");
        let block_size = logical_block_size(&raw_metadata).await;
        MetaReader::open(
            PageFileReader::from(raw_file, false, block_size),
            raw_metadata.len() as u32,
            *file_id,
        )
        .await
        .expect("open file reader error")
    }
}
