use std::{collections::HashMap, path::PathBuf};

use super::{
    constant::DEFAULT_BLOCK_SIZE, file_reader::MetaReader, types::split_page_addr, FileInfo,
    PageFileReader,
};
use crate::{
    env::Env,
    page_store::{NewFile, Result},
};

pub(crate) struct FileInfoBuilder<E: Env> {
    env: E,
    base: PathBuf,
    file_prefix: String,
}

impl<E: Env> FileInfoBuilder<E> {
    /// Create file info builder.
    /// it need base dir and page file name prefix.
    pub(crate) fn new(env: E, base: impl Into<PathBuf>, file_prefix: &str) -> Self {
        Self {
            env,
            base: base.into(),
            file_prefix: file_prefix.into(),
        }
    }

    /// Recovery file infos for specified file ids.
    /// It could be used to recovery in-memory `Version`'s `active_files` after
    /// it be recoveried from manifest files.
    pub(crate) async fn recovery_base_file_infos(
        &self,
        files: &[NewFile],
    ) -> Result<HashMap<u32, FileInfo>> {
        let mut file_infos = HashMap::with_capacity(files.len());
        let mut delete_pages_map = HashMap::new();
        for file in files {
            let (info, delete_pages) = self.recovery_one_file(file).await?;
            file_infos.insert(file.id, info);
            delete_pages_map.insert(file.id, delete_pages);
        }
        for (file_id, delete_pages) in delete_pages_map {
            Self::mantain_file_active_pages(&mut file_infos, file_id, &delete_pages);
        }
        Ok(file_infos)
    }

    // Add new file to exist file infos.
    // It copies the old file infos before add new file.
    // It could help version to prepare new Version's `active_files` after flush a
    // new page file.
    #[cfg(test)]
    pub(crate) fn add_file_info(
        &self,
        files: &HashMap<u32, FileInfo>,
        new_file_info: FileInfo,
        new_delete_pages: &[u64],
    ) -> Result<HashMap<u32, FileInfo>> {
        let file_id = new_file_info.get_file_id();
        let mut files = files.clone();
        files.insert(file_id, new_file_info);
        Self::mantain_file_active_pages(&mut files, file_id, new_delete_pages);
        Ok(files)
    }

    async fn recovery_one_file(&self, file: &NewFile) -> Result<(FileInfo, Vec<u64>)> {
        let meta_reader = self.open_meta_reader(&file.id).await;
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

        let delete_pages = meta_reader.read_delete_pages().await?;

        let active_size = meta.total_page_size();
        let info = FileInfo::new(active_pages, active_size, file.up1, file.up2, meta);

        Ok((info, delete_pages))
    }

    #[inline]
    fn mantain_file_active_pages(
        files: &mut HashMap<u32, FileInfo>,
        update_at: u32,
        delete_pages: &[u64],
    ) {
        for page_addr in delete_pages {
            let (file_id, _) = split_page_addr(*page_addr);
            if let Some(info) = files.get_mut(&file_id) {
                info.deactivate_page(update_at, *page_addr)
            }
        }
    }

    #[inline]
    async fn open_meta_reader(&self, file_id: &u32) -> MetaReader<E::PositionalReader> {
        let path = self.base.join(format!("{}_{file_id}", self.file_prefix));
        let raw_metadata = self
            .env
            .metadata(path.to_owned())
            .await
            .expect("read file metadata file");
        let raw_file = self
            .env
            .open_positional_reader(&path)
            .await
            .expect("file {path} not exist");
        MetaReader::open(
            PageFileReader::from(raw_file, false, DEFAULT_BLOCK_SIZE),
            raw_metadata.len as u32,
            *file_id,
        )
        .await
        .expect("open file reader error")
    }
}
