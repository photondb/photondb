use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use log::debug;

use super::{
    page_table::{PageTable, PageTableBuilder},
    version::DeltaVersion,
    FileInfo, NewFile, PageFiles, PageGroup, PageStore, Result, VersionEdit,
};
use crate::{env::Env, page_store::Manifest};

struct FileInfoBuilder<'a, E: Env> {
    facade: &'a PageFiles<E>,

    /// The virtual page file set.
    virtual_files: HashSet<u32>,
    /// The [`PageGroup`] for files.
    page_groups: HashMap<u32, PageGroup>,
    /// The [`FileInfo`] for files.
    file_infos: HashMap<u32, FileInfo>,
    /// The page table builder.
    page_table_builder: PageTableBuilder,

    /// Records the dealloc pages.
    dealloc_pages: HashMap<u32, Vec<u64>>,
}

struct FilesSummary {
    active_files: HashMap<u32, NewFile>,
    obsoleted_files: HashSet<u32>,
}

impl<E: Env> PageStore<E> {
    pub(super) async fn recover<P: AsRef<Path>>(
        env: E,
        path: P,
        options: &crate::PageStoreOptions,
    ) -> Result<(
        u32, /* next page file id */
        Manifest<E>,
        PageTable,
        PageFiles<E>,
        DeltaVersion,
    )> {
        let mut manifest = Manifest::open(env.to_owned(), path.as_ref()).await?;
        let versions = manifest.list_versions().await?;
        let summary = Self::apply_version_edits(versions);
        debug!("Recover with file summary {summary:?}");

        let page_files = PageFiles::new(env, path.as_ref(), options).await;

        let mut builder = FileInfoBuilder::new(&page_files);
        Self::recover_page_groups(&mut builder, &summary.active_files).await?;
        let (page_groups, file_infos, page_table) = builder.build();

        Self::delete_unreferenced_page_files(&page_files, &summary).await?;

        let next_file_id = summary.next_file_id();
        manifest.reset_next_file_id(summary.next_file_id());
        let delta = DeltaVersion {
            page_groups,
            file_infos,
            ..Default::default()
        };
        Ok((next_file_id, manifest, page_table, page_files, delta))
    }

    fn apply_version_edits(versions: Vec<VersionEdit>) -> FilesSummary {
        let mut active_files = HashMap::new();
        let mut obsoleted_files = HashSet::new();
        for edit in versions {
            if let Some(edit) = edit.file_stream {
                for file in edit.new_files {
                    active_files.insert(file.id, file);
                }
                for file in edit.deleted_files {
                    active_files.remove(&file);
                    obsoleted_files.insert(file);
                }
            }
        }

        FilesSummary {
            active_files,
            obsoleted_files,
        }
    }

    async fn recover_page_groups(
        builder: &mut FileInfoBuilder<'_, E>,
        active_files: &HashMap<u32, NewFile>,
    ) -> Result<()> {
        // ensure recover files in order.
        let mut files = active_files.values().cloned().collect::<Vec<_>>();
        files.sort_unstable();
        for file in files {
            builder.recover_file(file).await?;
        }
        Ok(())
    }

    async fn delete_unreferenced_page_files(
        page_files: &PageFiles<E>,
        summary: &FilesSummary,
    ) -> Result<()> {
        let exist_files = page_files.list_files()?;
        let deleted_files = Self::filter_obsoleted_files(
            exist_files,
            &summary.active_files,
            summary.obsoleted_files.clone(),
        );
        page_files.remove_files(deleted_files).await?;
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
            page_groups: HashMap::default(),
            file_infos: HashMap::default(),
            page_table_builder: PageTableBuilder::default(),
            dealloc_pages: HashMap::default(),
        }
    }

    /// Recover a file, the specified file id must be monotonically increasing.
    async fn recover_file(&mut self, file: NewFile) -> Result<()> {
        let meta_reader = self.facade.read_file_meta(file.id).await?;

        // 1. recover page groups
        for (&file_id, file_meta) in &meta_reader.page_groups {
            let file_info = PageGroup::new(file_meta.clone());
            self.virtual_files.insert(file_id);
            self.page_groups.insert(file_id, file_info);
        }

        // 2. recover file info.
        if !meta_reader.dealloc_pages.is_empty() {
            self.dealloc_pages
                .insert(file.id, meta_reader.dealloc_pages);
        }

        let file_meta = meta_reader.file_meta;
        self.file_infos
            .insert(file.id, FileInfo::new(file.up1, file.up2, file_meta));

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

    /// Build page groups, file infos, orphan page files, and page table.
    fn build(mut self) -> (HashMap<u32, PageGroup>, HashMap<u32, FileInfo>, PageTable) {
        self.maintain_active_pages();
        let page_table = self.page_table_builder.build();
        (self.page_groups, self.file_infos, page_table)
    }

    fn maintain_active_pages(&mut self) {
        let mut updates = self.dealloc_pages.keys().cloned().collect::<Vec<_>>();
        updates.sort_unstable();
        for updated_at in updates {
            let delete_pages = self.dealloc_pages.get(&updated_at).expect("Always exists");
            for &page_addr in delete_pages {
                let group_id = (page_addr >> 32) as u32;
                if let Some(info) = self.page_groups.get_mut(&group_id) {
                    info.deactivate_page(page_addr);
                    let physical_id = info.meta().file_id;
                    self.file_infos
                        .get_mut(&physical_id)
                        .expect("The file must exists")
                        .on_update(updated_at);
                }
            }
        }
    }
}

impl FilesSummary {
    fn next_file_id(&self) -> u32 {
        let val = std::cmp::max(
            self.active_files.keys().cloned().max().unwrap_or(0),
            self.obsoleted_files.iter().cloned().max().unwrap_or(0),
        );
        val + 1
    }
}

impl std::fmt::Debug for FilesSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilesSummary")
            .field("files", &self.active_files.keys())
            .field("obsoleted_files", &self.obsoleted_files)
            .finish()
    }
}
