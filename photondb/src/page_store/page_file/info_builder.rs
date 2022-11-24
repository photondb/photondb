use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use roaring::RoaringBitmap;

use super::{
    constant::DEFAULT_BLOCK_SIZE, file_reader::MetaReader, types::split_page_addr, FileInfo,
    FileMeta, MapFileInfo, MapFileMetaReader, MapFileReader, PageFileReader,
};
use crate::{
    env::Env,
    page_store::{NewFile, Result},
};

pub(crate) struct FileInfoBuilder<E: Env> {
    env: E,
    base: PathBuf,
}

impl<E: Env> FileInfoBuilder<E> {
    /// Create file info builder.
    /// it need base dir and page file name prefix.
    pub(crate) fn new(env: E, base: impl Into<PathBuf>) -> Self {
        Self {
            env,
            base: base.into(),
        }
    }

    /// Recovery page file infos for specified file ids.
    /// It could be used to recovery in-memory `Version`'s `active_files` after
    /// it be recoveried from manifest files.
    pub(crate) async fn recover_page_file_infos(
        &self,
        files: &[NewFile],
    ) -> Result<HashMap<u32, FileInfo>> {
        let mut file_infos = HashMap::with_capacity(files.len());
        let mut delete_pages_map = HashMap::new();
        for file in files {
            let (info, delete_pages) = self.recover_page_file(file).await?;
            file_infos.insert(file.id, info);
            delete_pages_map.insert(file.id, delete_pages);
        }
        for (file_id, delete_pages) in delete_pages_map {
            Self::mantain_file_active_pages(&mut file_infos, file_id, &delete_pages);
        }
        Ok(file_infos)
    }

    /// Like `recover_page_file_infos`, but recover map file infos.
    ///
    /// NOTE: Since the map file doesn't contains any dealloc pages record, so
    /// it should be recovered before page files, in order to maintain active
    /// pages.
    #[allow(unused)]
    pub(crate) async fn recover_map_file_infos(
        &self,
        files: &[NewFile],
    ) -> Result<(HashMap<u32, FileInfo>, HashMap<u32, MapFileInfo>)> {
        let mut virtual_infos = HashMap::default();
        let mut map_file_infos = HashMap::with_capacity(files.len());
        for file in files {
            let (sub_virtual_infos, file_info) = self.recover_map_file(file).await?;
            virtual_infos.extend(sub_virtual_infos.into_iter());
            map_file_infos.insert(file.id, file_info);
        }
        Ok((virtual_infos, map_file_infos))
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

    /// Read page file meta and construct [`FileInfo`] and dealloc pages.
    async fn recover_page_file(&self, file: &NewFile) -> Result<(FileInfo, Vec<u64>)> {
        let meta_reader = self.open_meta_reader(file.id).await?;
        let meta = meta_reader.file_metadata();

        let mut referenced_files = HashSet::new();
        let delete_pages = meta_reader.read_delete_pages().await?;
        for page_addr in &delete_pages {
            let (file_id, _) = split_page_addr(*page_addr);
            referenced_files.insert(file_id);
        }

        let active_pages = active_bitmap(&meta);
        let active_size = meta.total_page_size();
        let info = FileInfo::new(
            active_pages,
            active_size,
            file.up1,
            file.up2,
            referenced_files,
            meta,
        );

        Ok((info, delete_pages))
    }

    async fn recover_map_file(
        &self,
        file: &NewFile,
    ) -> Result<(HashMap<u32, FileInfo>, MapFileInfo)> {
        let meta_reader = self.open_map_file_meta_reader(file.id).await?;
        let file_meta = meta_reader.file_meta();
        let file_info = MapFileInfo::new(file.up1, file.up2, file_meta);
        let page_meta_map = meta_reader.page_file_meta_map();
        let mut virtual_infos = HashMap::with_capacity(page_meta_map.len());
        for (&file_id, file_meta) in page_meta_map {
            let active_pages = active_bitmap(file_meta);
            let active_size = file_meta.total_page_size();
            let file_info = FileInfo::new(
                active_pages,
                active_size,
                file.up1,
                file.up2,
                HashSet::default(),
                file_meta.clone(),
            );
            virtual_infos.insert(file_id, file_info);
        }
        Ok((virtual_infos, file_info))
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
    async fn open_meta_reader(&self, file_id: u32) -> Result<MetaReader<E::PositionalReader>> {
        let (reader, file_size) = self
            .open_positional_reader(super::facade::PAGE_FILE_PREFIX, file_id)
            .await?;
        MetaReader::open(
            Arc::new(PageFileReader::from(
                reader,
                false,
                DEFAULT_BLOCK_SIZE,
                file_size as usize,
            )),
            file_id,
        )
        .await
    }

    #[inline]
    async fn open_map_file_meta_reader(
        &self,
        file_id: u32,
    ) -> Result<MapFileMetaReader<E::PositionalReader>> {
        let (reader, file_size) = self
            .open_positional_reader(super::facade::MAP_FILE_PREFIX, file_id)
            .await?;
        let reader = MapFileReader::from(reader, false, DEFAULT_BLOCK_SIZE, file_size as usize);
        MapFileMetaReader::open(file_id, Arc::new(reader)).await
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
}

pub(super) fn active_bitmap(meta: &FileMeta) -> RoaringBitmap {
    let mut active_pages = roaring::RoaringBitmap::new();
    for page_addr in meta.data_offsets().keys() {
        let (_, index) = split_page_addr(*page_addr);
        active_pages.insert(index);
    }
    active_pages
}
