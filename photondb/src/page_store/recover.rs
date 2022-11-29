use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use log::debug;

use super::{
    page_table::{PageTable, PageTableBuilder},
    version::DeltaVersion,
    FileInfo, MapFileInfo, NewFile, PageFiles, PageStore, Result, VersionEdit,
};
use crate::{env::Env, page_store::Manifest};

struct FileInfoBuilder<'a, E: Env> {
    facade: &'a PageFiles<E>,

    /// The virtual page file set.
    virtual_files: HashSet<u32>,
    /// The [`FileInfo`] for page files, includes virtual page files.
    page_files: HashMap<u32, FileInfo>,
    /// The [`MapFileInfo`] for map files.
    map_files: HashMap<u32, MapFileInfo>,
    /// The page table builder.
    page_table_builder: PageTableBuilder,

    /// Records the dealloc pages.
    orphan_page_files: HashSet<u32>,
    dealloc_pages: HashMap<u32, Vec<u64>>,
}

struct FilesSummary {
    active_page_files: HashMap<u32, NewFile>,
    active_map_files: HashMap<u32, NewFile>,
    obsoleted_page_files: HashSet<u32>,
    obsoleted_map_files: HashSet<u32>,
}

impl<E: Env> PageStore<E> {
    pub(super) async fn recover<P: AsRef<Path>>(
        env: E,
        path: P,
        options: &crate::PageStoreOptions,
    ) -> Result<(
        u32, /* next page file id */
        u32, /* next map file id */
        Manifest<E>,
        PageTable,
        PageFiles<E>,
        DeltaVersion,
        HashSet<u32>, /* orphan page files */
    )> {
        let manifest = Manifest::open(env.to_owned(), path.as_ref()).await?;
        let versions = manifest.list_versions().await?;
        let summary = Self::apply_version_edits(versions);
        debug!("Recover with file summary {summary:?}");

        let page_files = PageFiles::new(env, path.as_ref(), options).await;

        // WARNING: recover map files before page files.
        let mut builder = FileInfoBuilder::new(&page_files);
        Self::recover_map_file_infos(&mut builder, &summary.active_map_files).await?;
        Self::recover_page_file_infos(&mut builder, &summary.active_page_files).await?;
        let (file_infos, map_files, orphan_page_files, page_table) = builder.build();

        Self::delete_unreferenced_page_files(&page_files, &summary).await?;

        let next_page_file_id = summary.next_page_file_id();
        let next_map_file_id = summary.next_map_file_id();
        let delta = DeltaVersion {
            page_files: file_infos,
            map_files,
            ..Default::default()
        };
        Ok((
            next_page_file_id,
            next_map_file_id,
            manifest,
            page_table,
            page_files,
            delta,
            orphan_page_files,
        ))
    }

    fn apply_version_edits(versions: Vec<VersionEdit>) -> FilesSummary {
        let mut active_page_files = HashMap::new();
        let mut active_map_files = HashMap::new();
        let mut obsoleted_page_files = HashSet::new();
        let mut obsoleted_map_files = HashSet::new();
        for edit in versions {
            if let Some(edit) = edit.page_stream {
                for file in edit.new_files {
                    active_page_files.insert(file.id, file);
                }
                for file in edit.deleted_files {
                    active_page_files.remove(&file);
                    obsoleted_page_files.insert(file);
                }
            }
            if let Some(edit) = edit.map_stream {
                for file in edit.new_files {
                    active_map_files.insert(file.id, file);
                }
                for file in edit.deleted_files {
                    active_map_files.remove(&file);
                    obsoleted_map_files.insert(file);
                }
            }
        }

        FilesSummary {
            active_page_files,
            active_map_files,
            obsoleted_page_files,
            obsoleted_map_files,
        }
    }

    async fn recover_page_file_infos(
        builder: &mut FileInfoBuilder<'_, E>,
        active_files: &HashMap<u32, NewFile>,
    ) -> Result<()> {
        // ensure recover files in order.
        let mut files = active_files.values().cloned().collect::<Vec<_>>();
        files.sort_unstable();
        for file in files {
            builder.recover_page_file(file).await?;
        }
        Ok(())
    }

    async fn recover_map_file_infos(
        builder: &mut FileInfoBuilder<'_, E>,
        active_files: &HashMap<u32, NewFile>,
    ) -> Result<()> {
        // ensure recover files in order.
        let mut files = active_files.values().cloned().collect::<Vec<_>>();
        files.sort_unstable();
        for file in files {
            builder.recover_map_file(file).await?;
        }
        Ok(())
    }

    async fn delete_unreferenced_page_files(
        page_files: &PageFiles<E>,
        summary: &FilesSummary,
    ) -> Result<()> {
        let exist_files = page_files.list_page_files()?;
        let deleted_files = Self::filter_obsoleted_files(
            exist_files,
            &summary.active_page_files,
            summary.obsoleted_page_files.clone(),
        );
        page_files.remove_page_files(deleted_files).await?;

        let exist_files = page_files.list_map_files()?;
        let deleted_files = Self::filter_obsoleted_files(
            exist_files,
            &summary.active_map_files,
            summary.obsoleted_map_files.clone(),
        );
        page_files.remove_map_files(deleted_files).await?;
        Ok(())
    }

    fn filter_obsoleted_files(
        exist_files: Vec<u32>,
        active_files: &HashMap<u32, NewFile>,
        obsoleted_files: HashSet<u32>,
    ) -> Vec<u32> {
        let mut obsoleted_files = obsoleted_files;
        let exist_files = exist_files.into_iter().collect::<HashSet<_>>();
        obsoleted_files.retain(|fid| exist_files.contains(fid));
        for file_id in exist_files {
            if !active_files.contains_key(&file_id) {
                obsoleted_files.insert(file_id);
            }
        }
        obsoleted_files.into_iter().collect::<Vec<_>>()
    }
}

impl<'a, E: Env> FileInfoBuilder<'a, E> {
    fn new(facade: &'a PageFiles<E>) -> Self {
        FileInfoBuilder {
            facade,
            virtual_files: HashSet::default(),
            page_files: HashMap::default(),
            map_files: HashMap::default(),
            page_table_builder: PageTableBuilder::default(),
            orphan_page_files: HashSet::default(),
            dealloc_pages: HashMap::default(),
        }
    }

    /// Recover a map file, the specified file id must be monotonically
    /// increasing.
    ///
    /// NOTE: Since the map file doesn't contains any dealloc pages record, so
    /// it should be recovered before page files, in order to maintain active
    /// pages.
    async fn recover_map_file(&mut self, file: NewFile) -> Result<()> {
        let meta_reader = self.facade.read_map_file_meta(file.id).await?;

        // 1. recover virtual file infos.
        for (&file_id, file_meta) in &meta_reader.file_meta_map {
            let offset = meta_reader
                .file_offset(file_id)
                .expect("File offset must exists") as usize;
            let active_pages = file_meta.pages_bitmap();
            let active_size = file_meta.total_page_size().saturating_sub(offset);
            let file_info = FileInfo::new(
                active_pages,
                active_size,
                file.up1,
                file.up2,
                HashSet::default(),
                file_meta.clone(),
            );
            self.virtual_files.insert(file_id);
            self.page_files.insert(file_id, file_info);
        }

        // 2. recover map file info.
        let file_meta = meta_reader.file_meta;
        self.map_files
            .insert(file.id, MapFileInfo::new(file.up1, file.up2, file_meta));

        // 3. recover page table.
        for (_, page_table) in meta_reader.page_tables {
            for (page_addr, page_id) in page_table {
                if self.page_table_builder.get(page_id) < page_addr {
                    self.page_table_builder.set(page_id, page_addr);
                }
            }
        }

        Ok(())
    }

    /// Recover a page file, the specified file id must be monotonically
    /// increasing.
    async fn recover_page_file(&mut self, file: NewFile) -> Result<()> {
        let file_id = file.id;
        let meta_reader = self.facade.open_page_file_meta_reader(file_id).await?;
        let file_meta = meta_reader.file_metadata();

        // 1. read dealloc pages.
        let delete_pages = meta_reader.read_delete_pages().await?;
        let mut referenced_files = HashSet::new();
        if !delete_pages.is_empty() {
            for page_addr in &delete_pages {
                referenced_files.insert((page_addr >> 32) as u32);
            }
            self.dealloc_pages.insert(file_id, delete_pages);
        }

        if self.virtual_files.contains(&file_id) {
            // This page file has compounded into a map file, which has been recovered
            // early.
            debug!("ignore page file {file_id} in recovery, since it has been compounded");
            if !self.dealloc_pages.is_empty() {
                self.orphan_page_files.insert(file_id);
            }
            return Ok(());
        }

        // 2. recover file info
        let active_pages = file_meta.pages_bitmap();
        let active_size = file_meta.total_page_size();
        let info = FileInfo::new(
            active_pages,
            active_size,
            file.up1,
            file.up2,
            referenced_files,
            file_meta,
        );
        self.page_files.insert(file_id, info);

        // 3. recover page table
        for (page_addr, page_id) in meta_reader.read_page_table().await? {
            if self.page_table_builder.get(page_id) < page_addr {
                self.page_table_builder.set(page_id, page_addr);
            }
        }

        Ok(())
    }

    /// Build page files, map files, orphan page files, and page table.
    fn build(
        mut self,
    ) -> (
        HashMap<u32, FileInfo>,
        HashMap<u32, MapFileInfo>,
        HashSet<u32>,
        PageTable,
    ) {
        self.maintain_active_pages();
        let page_table = self.page_table_builder.build();
        (
            self.page_files,
            self.map_files,
            self.orphan_page_files,
            page_table,
        )
    }

    fn maintain_active_pages(&mut self) {
        let mut updates = self.dealloc_pages.keys().cloned().collect::<Vec<_>>();
        updates.sort_unstable();
        for updated_at in updates {
            let delete_pages = self.dealloc_pages.get(&updated_at).expect("Always exists");
            for &page_addr in delete_pages {
                let file_id = (page_addr >> 32) as u32;
                if let Some(info) = self.page_files.get_mut(&file_id) {
                    info.deactivate_page(updated_at, page_addr);
                    if let Some(map_file_id) = info.get_map_file_id() {
                        self.map_files
                            .get_mut(&map_file_id)
                            .expect("The map file must exists")
                            .on_update(updated_at);
                    }
                }
            }
        }
    }
}

impl FilesSummary {
    fn next_page_file_id(&self) -> u32 {
        let val = std::cmp::max(
            self.active_page_files.keys().cloned().max().unwrap_or(0),
            self.obsoleted_page_files.iter().cloned().max().unwrap_or(0),
        );
        val + 1
    }

    fn next_map_file_id(&self) -> u32 {
        let val = std::cmp::max(
            self.active_map_files.keys().cloned().max().unwrap_or(0),
            self.obsoleted_map_files.iter().cloned().max().unwrap_or(0),
        );
        val + 1
    }
}

impl std::fmt::Debug for FilesSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilesSummary")
            .field("page_files", &self.active_page_files.keys())
            .field("map_files", &self.active_map_files.keys())
            .field("obsoleted_map_files", &self.obsoleted_map_files)
            .field("obsoleted_page_files", &self.obsoleted_page_files)
            .finish()
    }
}
