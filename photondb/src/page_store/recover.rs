use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use super::{page_table::PageTable, FileInfo, NewFile, PageFiles, PageStore, Result, VersionEdit};
use crate::{env::Env, page_store::Manifest};

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
        u32, /* next file id */
        Manifest<E>,
        PageTable,
        PageFiles<E>,
        HashMap<u32, FileInfo>,
    )> {
        let manifest = Manifest::open(env.to_owned(), path.as_ref()).await?;
        let versions = manifest.list_versions().await?;
        let summary = Self::apply_version_edits(versions);

        let page_files = PageFiles::new(env, path.as_ref(), options.use_direct_io).await;
        let file_infos = Self::recover_file_infos(&page_files, &summary.active_files).await?;
        let page_table = Self::recover_page_table(&page_files, &summary.active_files).await?;

        Self::delete_unreferenced_page_files(&page_files, &summary).await?;

        let next_file_id = summary.active_files.keys().cloned().max().unwrap_or(0) + 1;
        Ok((next_file_id, manifest, page_table, page_files, file_infos))
    }

    fn apply_version_edits(versions: Vec<VersionEdit>) -> FilesSummary {
        let mut files = HashMap::new();
        let mut deleted_files = HashSet::new();
        for edit in versions {
            for file in edit.new_files {
                files.insert(file.id, file);
            }
            for file in edit.deleted_files {
                files.remove(&file);
                deleted_files.insert(file);
            }
        }

        FilesSummary {
            active_files: files,
            obsoleted_files: deleted_files,
        }
    }

    async fn recover_file_infos(
        page_files: &PageFiles<E>,
        active_files: &HashMap<u32, NewFile>,
    ) -> Result<HashMap<u32, FileInfo>> {
        // ensure recover files in order.
        let mut files = active_files.values().cloned().collect::<Vec<_>>();
        files.sort_unstable();
        let builder = page_files.new_info_builder();
        builder.recovery_base_file_infos(&files).await
    }

    async fn recover_page_table(
        page_files: &PageFiles<E>,
        active_files: &HashMap<u32, NewFile>,
    ) -> Result<PageTable> {
        // ensure recover files in order.
        let mut files = active_files.keys().cloned().collect::<Vec<_>>();
        files.sort_unstable();

        let table = PageTable::default();
        for file_id in files {
            let meta_reader = page_files.open_meta_reader(file_id).await?;
            for (page_addr, page_id) in meta_reader.read_page_table().await? {
                table.set(page_id, page_addr);
            }
        }
        Ok(table)
    }

    async fn delete_unreferenced_page_files(
        page_files: &PageFiles<E>,
        summary: &FilesSummary,
    ) -> Result<()> {
        let mut obsoleted_files = summary.obsoleted_files.clone();
        for file_id in page_files.list_page_files()? {
            if !summary.active_files.contains_key(&file_id) {
                obsoleted_files.insert(file_id);
            }
        }
        let deleted_files = obsoleted_files.into_iter().collect::<Vec<_>>();
        page_files.remove_files(deleted_files).await?;
        Ok(())
    }
}
