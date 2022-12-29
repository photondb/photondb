use std::{sync::Arc, time::Instant};

use log::info;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    env::Env,
    page_store::{stats::AtomicJobStats, *},
    util::shutdown::{with_shutdown, Shutdown},
};

pub(crate) struct FlushCtx<E: Env> {
    options: Options,
    shutdown: Shutdown,
    job_stats: Arc<AtomicJobStats>,
    version_owner: Arc<VersionOwner>,
    page_files: Arc<PageFiles<E>>,
    manifest: Arc<futures::lock::Mutex<Manifest<E>>>,
}

#[derive(Default)]
struct FlushPageStats {
    data_size: usize,

    num_records: usize,
    num_tombstone_records: usize,
    num_dealloc_pages: usize,
    num_skip_pages: usize,
}

impl<E: Env> FlushCtx<E> {
    pub(crate) fn new(
        options: Options,
        shutdown: Shutdown,
        job_stats: Arc<AtomicJobStats>,
        version_owner: Arc<VersionOwner>,
        page_files: Arc<PageFiles<E>>,
        manifest: Arc<futures::lock::Mutex<Manifest<E>>>,
    ) -> Self {
        FlushCtx {
            options,
            shutdown,
            job_stats,
            version_owner,
            page_files,
            manifest,
        }
    }

    pub(crate) async fn run(mut self) {
        'OUTER: loop {
            let version = self.version_owner.current();
            let write_buffer = version.min_write_buffer();

            // If the [`WriteBuffer`] can be flushed, then it should continue because
            // [`Notify`] is single permits. But this may also lead to [`WriteBuffer`]
            // flushed but notified is not consumed, so loop detection is required.
            while !write_buffer.is_flushable() {
                if with_shutdown(&mut self.shutdown, version.buffer_set.wait_flushable())
                    .await
                    .is_none()
                {
                    break 'OUTER;
                }
            }

            match self.flush(write_buffer.as_ref()).await {
                Ok(()) => {}
                Err(err) => {
                    todo!("flush write buffer: {err:?}");
                }
            }
        }

        if !self.options.avoid_flush_during_shutdown {
            self.flush_during_shutdown().await;
        }
    }

    /// Flush write buffers when user try to shutdown a page store.
    ///
    /// Note: it assumes that there no any inflights writers during shutdown.
    async fn flush_during_shutdown(self) {
        let buffers_range = { self.version_owner.current().buffers_range() };
        for id in buffers_range {
            let buffer = {
                let version = self.version_owner.current();
                version
                    .buffer_set
                    .get(id)
                    .expect("The target write buffer must exists")
                    .clone()
            };
            if !buffer.is_sealed() {
                if buffer.is_empty() {
                    // skip empty buffer.
                    break;
                }
                let _ = buffer.seal();
            }
            assert!(buffer.is_flushable());
            self.flush_impl(&buffer, false)
                .await
                .expect("TODO: flush write buffer");
        }
    }

    #[inline]
    async fn flush(&self, write_buffer: &WriteBuffer) -> Result<()> {
        self.flush_impl(write_buffer, true).await
    }

    async fn flush_impl(&self, write_buffer: &WriteBuffer, wait: bool) -> Result<()> {
        let start_at = Instant::now();
        let group_id = write_buffer.group_id();
        let (dealloc_pages, page_group, file_info) = self.build_page_file(write_buffer).await?;

        let file_id = file_info.meta().file_id;
        let file_size = file_info.meta().file_size;
        info!(
            "Flush page group {group_id} output file {file_id} with {file_size} bytes, \
                {} active pages, {} dealloc pages, lasted {} microseconds",
            page_group.num_active_pages(),
            dealloc_pages.len(),
            start_at.elapsed().as_micros()
        );

        self.save_and_install_version(page_group, file_info, dealloc_pages, wait)
            .await?;

        write_buffer.on_flushed();

        Ok(())
    }

    async fn save_and_install_version(
        &self,
        page_group: PageGroup,
        file_info: FileInfo,
        dealloc_pages: Vec<u64>,
        wait_new_buffer: bool,
    ) -> Result<()> {
        let mut manifest = self.manifest.lock().await;
        let version = self.version_owner.current();

        let now = manifest.now();
        let (mut page_groups, mut file_infos) =
            self.apply_dealloc_pages(&version, now, dealloc_pages.to_owned());
        let obsoleted_files = drain_obsoleted_files(&mut page_groups, &mut file_infos);

        let group_id = page_group.meta().group_id;
        let file_id = file_info.meta().file_id;
        if !page_group.is_empty() {
            page_groups.insert(group_id, page_group);
        }
        file_infos.insert(file_id, file_info);

        let edit = make_flush_version_edit(file_id, &obsoleted_files);
        manifest
            .record_version_edit(edit, || version_snapshot(&version))
            .await?;

        // Release buffer permit and ensure the new buffer is installed, before install
        // new version.
        if wait_new_buffer {
            version.buffer_set.release_permit_and_wait(group_id).await;
        }

        let delta = DeltaVersion {
            reason: VersionUpdateReason::Flush,
            page_groups,
            file_infos,
            obsoleted_files,
        };
        // Safety: the mutable reference of [`Manifest`] is hold.
        unsafe { self.version_owner.install(delta) };
        Ok(())
    }

    /// Flush [`WriteBuffer`] to files and returns dealloc pages.
    async fn build_page_file(
        &self,
        write_buffer: &WriteBuffer,
    ) -> Result<(Vec<u64>, PageGroup, FileInfo)> {
        assert!(write_buffer.is_flushable());

        let mut flush_stats = FlushPageStats::default();
        let (dealloc_pages, skip_pages) =
            collect_dealloc_pages_and_stats(write_buffer, &mut flush_stats);

        let group_id = write_buffer.group_id();
        info!("Flush write buffer {group_id} to file, {flush_stats}");

        let file_id = {
            let mut lock = self.manifest.lock().await;
            lock.next_file_id()
        };
        let mut builder = self
            .page_files
            .new_file_builder(
                file_id,
                self.options.compression_on_flush,
                self.options.page_checksum_type,
            )
            .await?;
        let mut group_builder = builder.add_page_group(group_id);
        let mut write_bytes = 0;
        let mut discard_bytes = 0;
        for (page_addr, header, record_ref) in write_buffer.iter() {
            if let RecordRef::Page(page) = record_ref {
                if header.is_tombstone() || skip_pages.contains(&(page_addr as u32)) {
                    discard_bytes += header.page_size();
                    continue;
                }
                let content = page.data();
                group_builder
                    .add_page(header.page_id(), page_addr, page.info(), content)
                    .await?;
                write_bytes += content.len();
                let _ = self.page_files.populate_cache(page_addr, content);
            }
        }
        group_builder.add_dealloc_pages(&dealloc_pages);
        builder = group_builder.finish().await?;
        let (page_groups, file_info) = builder.finish(file_id).await?;
        let page_group = page_groups.get(&group_id).unwrap().clone();

        self.job_stats.flush_write_bytes.add(write_bytes as u64);
        self.job_stats.flush_discard_bytes.add(discard_bytes as u64);

        Ok((dealloc_pages, page_group, file_info))
    }

    fn apply_dealloc_pages(
        &self,
        version: &Version,
        now: u32,
        dealloc_pages: Vec<u64>,
    ) -> (FxHashMap<u32, PageGroup>, FxHashMap<u32, FileInfo>) {
        let mut page_groups = version.page_groups().clone();
        let mut file_infos = version.file_infos().clone();
        let mut updated_files = FxHashSet::default();
        for page_addr in dealloc_pages {
            let group_id = (page_addr >> 32) as u32;
            if let Some(page_group) = page_groups.get_mut(&group_id) {
                if !page_group.deactivate_page(page_addr) {
                    continue;
                }

                updated_files.insert(page_group.meta().file_id);
            };
        }
        for file_id in updated_files {
            if let Some(file) = file_infos.get_mut(&file_id) {
                file.on_update(now);
            }
        }

        (page_groups, file_infos)
    }
}

impl std::fmt::Display for FlushPageStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "num_records: {} num_tombstone_records: {} num_dealloc_pages: {} num_skip_pages: {} data_size: {}",
            self.num_records, self.num_tombstone_records, self.num_dealloc_pages, self.num_skip_pages, self.data_size
        )
    }
}

fn drain_obsoleted_files(
    page_groups: &mut FxHashMap<u32, PageGroup>,
    file_infos: &mut FxHashMap<u32, FileInfo>,
) -> FxHashSet<u32> {
    // remove empty page groups.
    page_groups.retain(|_, g| !g.is_empty());

    let mut empty_files = FxHashSet::default();
    'OUTER: for (file_id, info) in &mut *file_infos {
        for group_id in info.meta().page_groups.keys() {
            if let Some(page_group) = page_groups.get(group_id) {
                if !page_group.is_empty() {
                    // page group exists and not empty.
                    continue 'OUTER;
                }
            }
        }
        empty_files.insert(*file_id);
    }

    let mut obsoleted_files = FxHashSet::default();
    'OUTER: for file_id in empty_files {
        let file_info = file_infos.get_mut(&file_id).unwrap();
        for group_id in &file_info.meta().referenced_groups {
            // Is the referenced group empty? (empty group are removed in above)
            if page_groups.contains_key(group_id) {
                continue 'OUTER;
            }
        }

        file_infos.remove(&file_id);
        obsoleted_files.insert(file_id);
    }
    obsoleted_files
}

fn collect_dealloc_pages_and_stats(
    write_buffer: &WriteBuffer,
    flush_stats: &mut FlushPageStats,
) -> (Vec<u64>, FxHashSet<u32>) {
    let file_id = write_buffer.group_id();
    let mut dealloc_pages = Vec::new();
    let mut skip_pages = FxHashSet::default();
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
                    .filter(|&&v| (v >> 32) as u32 == file_id)
                    .map(|&v| v as u32),
            );
        }
    }

    flush_stats.num_skip_pages = skip_pages.len();
    flush_stats.num_dealloc_pages = dealloc_pages.len();

    (dealloc_pages, skip_pages)
}

pub(super) fn version_snapshot(version: &Version) -> VersionEdit {
    let new_files: Vec<NewFile> = version
        .file_infos()
        .values()
        .map(Into::into)
        .collect::<Vec<_>>();

    // NOTE: Only the deleted files of the current version are recorded here, and
    // the files of previous versions are not recorded here.
    let deleted_files = version.obsoleted_files();
    let stream = StreamEdit {
        new_files,
        deleted_files,
    };
    VersionEdit {
        file_stream: Some(stream),
    }
}

fn make_flush_version_edit(file_id: u32, obsoleted_files: &FxHashSet<u32>) -> VersionEdit {
    let deleted_files = obsoleted_files.iter().cloned().collect();
    let new_files = vec![NewFile::from(file_id)];
    let stream = StreamEdit {
        new_files,
        deleted_files,
    };
    VersionEdit {
        file_stream: Some(stream),
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use super::FlushCtx;
    use crate::{
        env::Photon,
        page_store::{
            version::{DeltaVersion, Version, VersionOwner},
            Manifest, PageFiles, WriteBuffer,
        },
        util::shutdown::ShutdownNotifier,
        PageStoreOptions,
    };

    async fn new_flush_ctx(base: &Path) -> FlushCtx<Photon> {
        std::fs::create_dir_all(base).unwrap();
        let notifier = ShutdownNotifier::default();
        let shutdown = notifier.subscribe();
        let version = Version::new(1 << 16, 1, 8, DeltaVersion::default());
        let version_owner = Arc::new(VersionOwner::new(version));
        let opt = PageStoreOptions {
            cache_capacity: 2 << 10,
            ..Default::default()
        };
        FlushCtx {
            options: opt.to_owned(),
            shutdown,
            job_stats: Arc::default(),
            version_owner,
            page_files: Arc::new(PageFiles::new(Photon, base, &opt).await),
            manifest: Arc::new(futures::lock::Mutex::new(
                Manifest::open(Photon, base).await.unwrap(),
            )),
        }
    }

    #[photonio::test]
    async fn flush_write_buffer_local_pages_ignore() {
        let base = tempdir::TempDir::new("flush_ignore_local_pages").unwrap();
        let ctx = new_flush_ctx(base.path()).await;
        let wb = WriteBuffer::with_capacity(1, 1 << 16);
        unsafe {
            let (addr, _, _) = wb.alloc_page(1, 123, false).unwrap();
            wb.dealloc_pages(&[addr], false).unwrap();
            wb.seal().unwrap();
            let (deleted_pages, file_info, _) = ctx.build_page_file(&wb).await.unwrap();
            assert!(deleted_pages.is_empty());
            assert!(!file_info.is_page_active(addr));
            assert!(file_info.get_page_handle(addr).is_none())
        }
    }

    #[photonio::test]
    async fn flush_write_buffer_local_pages_aborted_skip() {
        let base = tempdir::TempDir::new("flush_ignore_local_pages").unwrap();
        let ctx = new_flush_ctx(base.path()).await;
        let wb = WriteBuffer::with_capacity(1, 1 << 16);
        unsafe {
            let (addr, header, _) = wb.alloc_page(1, 123, false).unwrap();
            header.set_tombstone();
            wb.seal().unwrap();
            let (deleted_pages, file_info, _) = ctx.build_page_file(&wb).await.unwrap();
            assert!(deleted_pages.is_empty());
            assert!(!file_info.is_page_active(addr));
            assert!(file_info.get_page_handle(addr).is_none());
        }
    }
}
