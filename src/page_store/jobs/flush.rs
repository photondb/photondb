use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::page_store::{
    version::DeltaVersion, FileInfo, Manifest, PageFiles, RecordRef, Result, Version, VersionEdit,
    WriteBuffer,
};

pub(crate) struct FlushCtx {
    // TODO: cancel task
    global_version: Arc<Mutex<Version>>,
    page_files: Arc<PageFiles>,
    manifest: Arc<futures::lock::Mutex<Manifest>>,
}

impl FlushCtx {
    pub(crate) fn new(
        global_version: Arc<Mutex<Version>>,
        page_files: Arc<PageFiles>,
        manifest: Arc<futures::lock::Mutex<Manifest>>,
    ) -> Self {
        FlushCtx {
            global_version,
            page_files,
            manifest,
        }
    }

    pub(crate) async fn run(self) {
        loop {
            let version = self.version();
            let write_buffer = {
                let current = version.buffer_set.current();
                let file_id = current.min_file_id();
                current
                    .write_buffer(file_id)
                    .expect("WriteBuffer must exists")
                    .clone()
            };

            // If the [`WriteBuffer`] can be flushed, then it should continue because
            // [`Notify`] is single permits. But this may also lead to [`WriteBuffer`]
            // flushed but notified is not consumed, so loop detection is required.
            while !write_buffer.is_flushable() {
                version.buffer_set.wait_flushable().await;
            }

            match self.flush(&version, write_buffer.as_ref()).await {
                Ok(()) => {
                    self.refresh_version();
                }
                Err(err) => {
                    todo!("flush write buffer: {err}");
                }
            }
        }
    }

    async fn flush(&self, version: &Version, write_buffer: &WriteBuffer) -> Result<()> {
        let (deleted_pages, file_info) = self.build_page_file(write_buffer).await?;
        let files = self.apply_deleted_pages(version, deleted_pages);
        let file_id = write_buffer.file_id();

        let mut files = files;
        let deleted_files = files
            .drain_filter(|_, info| info.is_empty())
            .map(|(file_id, _)| file_id)
            .collect::<HashSet<_>>();
        files.insert(file_id, file_info);

        self.save_version_edit(version, file_id, &deleted_files)
            .await?;

        let delta = DeltaVersion {
            files,
            deleted_files,
        };

        let buffer_set = version.buffer_set.clone();
        let version = Rc::new(version.clone());
        Version::install(version, delta)?;
        buffer_set.on_flushed(file_id);
        Ok(())
    }

    /// Flush [`WriteBuffer`] to page files and returns deleted pages.
    async fn build_page_file(&self, write_buffer: &WriteBuffer) -> Result<(Vec<u64>, FileInfo)> {
        assert!(write_buffer.is_flushable());
        let file_id = write_buffer.file_id();
        let mut deleted_pages = Vec::default();
        let mut builder = self.page_files.new_file_builder(file_id).await?;
        for (page_addr, header, record_ref) in write_buffer.iter() {
            match record_ref {
                RecordRef::DeletedPages(pages) => {
                    builder.add_delete_pages(pages.as_slice());
                    deleted_pages.extend_from_slice(pages.as_slice());
                }
                RecordRef::Page(page) => {
                    let content = page.data();
                    builder
                        .add_page(header.page_id(), page_addr, content)
                        .await?;
                }
            }
        }
        let file_info = builder.finish().await?;
        Ok((deleted_pages, file_info))
    }

    async fn save_version_edit(
        &self,
        version: &Version,
        file_id: u32,
        deleted_files: &HashSet<u32>,
    ) -> Result<()> {
        let deleted_files = deleted_files.iter().cloned().collect();
        let edit = VersionEdit {
            new_files: vec![file_id],
            deleted_files,
        };

        let mut manifest = self.manifest.lock().await;
        manifest
            .record_version_edit(edit, || version_snapshot(version))
            .await
    }

    fn apply_deleted_pages(
        &self,
        version: &Version,
        deleted_pages: Vec<u64>,
    ) -> HashMap<u32, FileInfo> {
        let mut files = version.files().clone();
        for page_addr in deleted_pages {
            let file_id = (page_addr >> 32) as u32;
            let file_info = files.get_mut(&file_id).expect("File is missing");
            file_info.deactivate_page(page_addr);
        }
        files
    }

    fn version(&self) -> Version {
        self.global_version.lock().expect("Poisoned").clone()
    }

    fn refresh_version(&self) {
        let mut version = self.global_version.lock().expect("Poisoned");
        if let Some(new) = version.refresh() {
            *version = <Version as Clone>::clone(&new);
        }
    }
}

fn version_snapshot(version: &Version) -> VersionEdit {
    let new_files = version.files().keys().cloned().collect::<Vec<_>>();
    // FIXME: only the deleted files of the current version are recorded here, and
    // the files of previous versions are not recorded here.
    let deleted_files = version.deleted_files().clone();
    VersionEdit {
        new_files,
        deleted_files,
    }
}
