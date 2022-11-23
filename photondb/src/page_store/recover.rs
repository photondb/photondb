use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use super::{
    page_table::{PageTable, PageTableBuilder},
    version::DeltaVersion,
    FileInfo, MapFileInfo, NewFile, PageFiles, PageStore, Result, VersionEdit,
};
use crate::{env::Env, page_store::Manifest};

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
        u32, /* next file id */
        Manifest<E>,
        PageTable,
        PageFiles<E>,
        DeltaVersion,
    )> {
        let manifest = Manifest::open(env.to_owned(), path.as_ref()).await?;
        let versions = manifest.list_versions().await?;
        let summary = Self::apply_version_edits(versions);

        let page_files = PageFiles::new(env, path.as_ref(), options).await;
        let file_infos =
            Self::recover_page_file_infos(&page_files, &summary.active_page_files).await?;
        let map_files =
            Self::recover_map_file_infos(&page_files, &summary.active_map_files).await?;
        let page_table = Self::recover_page_table(&page_files, &summary.active_page_files).await?;

        Self::delete_unreferenced_page_files(&page_files, &summary).await?;

        let next_file_id = summary.active_page_files.keys().cloned().max().unwrap_or(0) + 1;
        let delta = DeltaVersion {
            page_files: file_infos,
            map_files,
            ..Default::default()
        };
        Ok((next_file_id, manifest, page_table, page_files, delta))
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
        page_files: &PageFiles<E>,
        active_files: &HashMap<u32, NewFile>,
    ) -> Result<HashMap<u32, FileInfo>> {
        // ensure recover files in order.
        let mut files = active_files.values().cloned().collect::<Vec<_>>();
        files.sort_unstable();
        let builder = page_files.new_info_builder();
        builder.recover_page_file_infos(&files).await
    }

    async fn recover_map_file_infos(
        page_files: &PageFiles<E>,
        active_files: &HashMap<u32, NewFile>,
    ) -> Result<HashMap<u32, MapFileInfo>> {
        // ensure recover files in order.
        let mut files = active_files.values().cloned().collect::<Vec<_>>();
        files.sort_unstable();

        // TODO: recover map files
        let _ = page_files;
        Ok(HashMap::default())
    }

    async fn recover_page_table(
        page_files: &PageFiles<E>,
        active_files: &HashMap<u32, NewFile>,
    ) -> Result<PageTable> {
        // ensure recover files in order.
        let mut files = active_files.keys().cloned().collect::<Vec<_>>();
        files.sort_unstable();

        let mut table = PageTableBuilder::default();
        for file_id in files {
            let meta_reader = page_files.open_page_file_meta_reader(file_id).await?;
            for (page_addr, page_id) in meta_reader.read_page_table().await? {
                table.set(page_id, page_addr);
            }
        }
        Ok(table.build())
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
