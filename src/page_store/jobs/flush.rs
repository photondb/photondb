use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use log::info;

use crate::{
    env::Env,
    page_store::{
        version::{DeltaVersion, Version},
        *,
    },
    util::shutdown::{with_shutdown, Shutdown},
};

pub(crate) struct FlushCtx<E: Env> {
    shutdown: Shutdown,
    global_version: Arc<Mutex<Version>>,
    page_files: Arc<PageFiles<E>>,
    manifest: Arc<futures::lock::Mutex<Manifest<E>>>,
}

#[derive(Default)]
struct FlushPageStats {
    data_size: usize,

    num_records: usize,
    num_tombstone_records: usize,
    num_dealloc_pages: usize,
    num_recycle_pages: usize,
}

impl<E: Env> FlushCtx<E> {
    pub(crate) fn new(
        shutdown: Shutdown,
        global_version: Arc<Mutex<Version>>,
        page_files: Arc<PageFiles<E>>,
        manifest: Arc<futures::lock::Mutex<Manifest<E>>>,
    ) -> Self {
        FlushCtx {
            shutdown,
            global_version,
            page_files,
            manifest,
        }
    }

    pub(crate) async fn run(mut self) {
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
                if with_shutdown(&mut self.shutdown, version.buffer_set.wait_flushable())
                    .await
                    .is_none()
                {
                    return;
                }
            }

            match self.flush(&version, write_buffer.as_ref()).await {
                Ok(()) => {
                    self.refresh_version();
                }
                Err(err) => {
                    todo!("flush write buffer: {err:?}");
                }
            }
        }
    }

    async fn flush(&self, version: &Version, write_buffer: &WriteBuffer) -> Result<()> {
        let start_at = Instant::now();
        let file_id = write_buffer.file_id();
        let (deleted_pages, file_info) = self.build_page_file(write_buffer).await?;

        info!(
            "Flush file {file_id} with {} bytes, {} active pages, lasted {} microseconds",
            file_info.file_size(),
            file_info.num_active_pages(),
            start_at.elapsed().as_micros()
        );

        let files = self.apply_deleted_pages(version, file_id, deleted_pages);

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
        Version::install(version, delta)?;
        buffer_set.on_flushed(file_id);
        Ok(())
    }

    /// Flush [`WriteBuffer`] to page files and returns deleted pages.
    async fn build_page_file(&self, write_buffer: &WriteBuffer) -> Result<(Vec<u64>, FileInfo)> {
        assert!(write_buffer.is_flushable());

        let mut flush_stats = FlushPageStats::default();
        let (dealloc_pages, skip_pages) =
            collect_dealloc_pages_and_stats(write_buffer, &mut flush_stats);

        let file_id = write_buffer.file_id();
        info!("Flush write buffer {file_id} {flush_stats}");

        let mut builder = self.page_files.new_file_builder(file_id).await?;
        for (page_addr, header, record_ref) in write_buffer.iter() {
            match record_ref {
                RecordRef::DeallocPages(_) => {}
                RecordRef::Page(page) => {
                    if header.is_tombstone() || skip_pages.contains(&page_addr) {
                        continue;
                    }
                    let content = page.data();
                    builder
                        .add_page(header.page_id(), page_addr, content)
                        .await?;
                }
            }
        }
        builder.add_delete_pages(&dealloc_pages);
        let file_info = builder.finish().await?;
        Ok((dealloc_pages, file_info))
    }

    async fn save_version_edit(
        &self,
        version: &Version,
        file_id: u32,
        deleted_files: &HashSet<u32>,
    ) -> Result<()> {
        let deleted_files = deleted_files.iter().cloned().collect();
        let edit = VersionEdit {
            new_files: vec![NewFile {
                id: file_id,
                up1: file_id,
                up2: file_id,
            }],
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
        new_file_id: u32,
        deleted_pages: Vec<u64>,
    ) -> HashMap<u32, FileInfo> {
        let mut files = version.files().clone();
        for page_addr in deleted_pages {
            let file_id = (page_addr >> 32) as u32;
            let Some(file_info) = files.get_mut(&file_id) else {
                panic!("file {file_id} is missing");
            };
            file_info.deactivate_page(new_file_id, page_addr);
        }
        files
    }

    fn version(&self) -> Version {
        self.global_version.lock().expect("Poisoned").clone()
    }

    fn refresh_version(&self) {
        let mut version = self.global_version.lock().expect("Poisoned");
        if let Some(new) = version.refresh() {
            *version = new;
        }
    }
}

impl std::fmt::Display for FlushPageStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "num_records: {} num_tombstone_records: {} num_dealloc_pages: {} num_recycle_pages: {} data_size: {}",
            self.num_records, self.num_tombstone_records, self.num_dealloc_pages, self.num_recycle_pages, self.data_size
        )
    }
}

fn collect_dealloc_pages_and_stats(
    write_buffer: &WriteBuffer,
    flush_stats: &mut FlushPageStats,
) -> (Vec<u64>, HashSet<u64>) {
    let file_id = write_buffer.file_id();
    let mut dealloc_pages = Vec::new();
    let mut skip_pages = HashSet::new();
    for (_, header, record_ref) in write_buffer.iter() {
        flush_stats.num_records += 1;
        if header.is_tombstone() {
            flush_stats.num_tombstone_records += 1;
        }

        flush_stats.data_size += header.page_size();
        if let RecordRef::DeallocPages(pages) = record_ref {
            dealloc_pages.extend(
                pages
                    .as_slice()
                    .iter()
                    .filter(|&&v| (v >> 32) as u32 != file_id),
            );
            skip_pages.extend(
                pages
                    .as_slice()
                    .iter()
                    .filter(|&&v| (v >> 32) as u32 == file_id),
            );
        }
    }

    flush_stats.num_recycle_pages = skip_pages.len();
    flush_stats.num_dealloc_pages = dealloc_pages.len();

    (dealloc_pages, skip_pages)
}

fn version_snapshot(version: &Version) -> VersionEdit {
    let new_files: Vec<NewFile> = version.files().values().map(Into::into).collect::<Vec<_>>();

    // FIXME: only the deleted files of the current version are recorded here, and
    // the files of previous versions are not recorded here.
    let deleted_files = version.deleted_files();
    VersionEdit {
        new_files,
        deleted_files,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        path::Path,
        sync::Arc,
    };

    use super::FlushCtx;
    use crate::{
        env::Photon,
        page_store::{version::Version, Manifest, PageFiles, WriteBuffer},
        util::shutdown::ShutdownNotifier,
    };

    async fn new_flush_ctx(base: &Path) -> FlushCtx<Photon> {
        std::fs::create_dir_all(base).unwrap();
        let notifier = ShutdownNotifier::default();
        let shutdown = notifier.subscribe();
        let version = Version::new(1 << 16, 1, HashMap::default(), HashSet::new());
        FlushCtx {
            shutdown,
            global_version: Arc::new(std::sync::Mutex::new(version)),
            page_files: Arc::new(PageFiles::new(Photon, base, "prefix").await),
            manifest: Arc::new(futures::lock::Mutex::new(
                Manifest::open(Photon, base).await.unwrap(),
            )),
        }
    }

    #[photonio::test]
    async fn flush_write_buffer_local_pages_ignore() {
        let base = std::env::temp_dir().join("flush_ignore_local_pages");
        let ctx = new_flush_ctx(&base).await;
        let wb = WriteBuffer::with_capacity(1, 1 << 16);
        unsafe {
            let (addr, _, _) = wb.alloc_page(1, 123, false).unwrap();
            wb.dealloc_pages(&[addr], false).unwrap();
            wb.seal().unwrap();
            let (deleted_pages, file_info) = ctx.build_page_file(&wb).await.unwrap();
            assert!(deleted_pages.is_empty());
            assert!(!file_info.is_page_active(addr));
        }
    }

    #[photonio::test]
    async fn flush_write_buffer_local_pages_aborted_skip() {
        let base = std::env::temp_dir().join("flush_ignore_local_pages");
        let ctx = new_flush_ctx(&base).await;
        let wb = WriteBuffer::with_capacity(1, 1 << 16);
        unsafe {
            let (addr, header, _) = wb.alloc_page(1, 123, false).unwrap();
            header.set_tombstone();
            wb.seal().unwrap();
            let (deleted_pages, file_info) = ctx.build_page_file(&wb).await.unwrap();
            assert!(deleted_pages.is_empty());
            assert!(!file_info.is_page_active(addr));
        }
    }
}
