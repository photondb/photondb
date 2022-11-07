use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
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
        shutdown: Shutdown,
        version_owner: Arc<VersionOwner>,
        page_files: Arc<PageFiles<E>>,
        manifest: Arc<futures::lock::Mutex<Manifest<E>>>,
    ) -> Self {
        FlushCtx {
            shutdown,
            version_owner,
            page_files,
            manifest,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let version = self.version_owner.current();
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
                Ok(()) => {}
                Err(err) => {
                    todo!("flush write buffer: {err:?}");
                }
            }
        }
    }

    async fn flush(&self, version: &Version, write_buffer: &WriteBuffer) -> Result<()> {
        let start_at = Instant::now();
        let file_id = write_buffer.file_id();
        let (dealloc_pages, file_info, reclaimed_files) =
            self.build_page_file(write_buffer).await?;

        info!(
            "Flush output file {file_id} with {} bytes, {} active pages, {} dealloc pages, lasted {} microseconds",
            file_info.file_size(),
            file_info.num_active_pages(),
            dealloc_pages.len(),
            start_at.elapsed().as_micros()
        );

        let mut files = self.apply_dealloc_pages(version, file_id, dealloc_pages);
        let obsoleted_files = drain_obsoleted_files(&mut files, reclaimed_files);
        files.insert(file_id, file_info);

        self.save_version_edit(version, file_id, &obsoleted_files)
            .await?;

        let delta = DeltaVersion {
            files,
            obsoleted_files,
        };

        self.version_owner.install(delta);
        version.buffer_set.on_flushed(file_id);
        Ok(())
    }

    /// Flush [`WriteBuffer`] to page files and returns dealloc pages.
    async fn build_page_file(
        &self,
        write_buffer: &WriteBuffer,
    ) -> Result<(Vec<u64>, FileInfo, HashSet<u32>)> {
        assert!(write_buffer.is_flushable());

        let mut flush_stats = FlushPageStats::default();
        let (dealloc_pages, skip_pages, reclaimed_files) =
            collect_dealloc_pages_and_stats(write_buffer, &mut flush_stats);

        let file_id = write_buffer.file_id();
        info!("Flush write buffer {file_id} to page file, {flush_stats}");

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
        Ok((dealloc_pages, file_info, reclaimed_files))
    }

    async fn save_version_edit(
        &self,
        version: &Version,
        file_id: u32,
        obsoleted_files: &HashSet<u32>,
    ) -> Result<()> {
        let deleted_files = obsoleted_files.iter().cloned().collect();
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

    fn apply_dealloc_pages(
        &self,
        version: &Version,
        new_file_id: u32,
        dealloc_pages: Vec<u64>,
    ) -> HashMap<u32, FileInfo> {
        let mut files = version.files().clone();
        for page_addr in dealloc_pages {
            let file_id = (page_addr >> 32) as u32;
            let Some(file_info) = files.get_mut(&file_id) else {
                panic!("file {file_id} is missing");
            };
            file_info.deactivate_page(new_file_id, page_addr);
        }
        files
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

/// Drain and return the obsoleted files from active files.
///
/// There exists two obsoleted sources:
/// 1. file is reclaimed (no active pages and dealloc pages has been rewritten).
/// 2. all referenced files (dealloc pages referenced to) is not belongs to
/// active files.
fn drain_obsoleted_files(
    files: &mut HashMap<u32, FileInfo>,
    reclaimed_files: HashSet<u32>,
) -> HashSet<u32> {
    assert_files_are_deletable(files, &reclaimed_files);
    let active_files = files
        .keys()
        .filter(|&id| !reclaimed_files.contains(id))
        .cloned()
        .collect::<HashSet<_>>();
    let mut obsoleted_files = reclaimed_files;
    for (id, info) in &mut *files {
        if info.is_obsoleted(&active_files) {
            obsoleted_files.insert(*id);
        }
    }
    files.drain_filter(|id, _| obsoleted_files.contains(id));
    obsoleted_files
}

fn assert_files_are_deletable(files: &HashMap<u32, FileInfo>, reclaimed_files: &HashSet<u32>) {
    for file_id in reclaimed_files {
        match files.get(file_id) {
            None => {
                panic!("The reclaimed file {file_id} is not a active file");
            }
            Some(info) => {
                if !info.is_empty() {
                    panic!("The reclamied file {file_id} has active pages");
                }
            }
        }
    }
}

fn collect_dealloc_pages_and_stats(
    write_buffer: &WriteBuffer,
    flush_stats: &mut FlushPageStats,
) -> (Vec<u64>, HashSet<u64>, HashSet<u32>) {
    let file_id = write_buffer.file_id();
    let mut dealloc_pages = Vec::new();
    let mut skip_pages = HashSet::new();
    let mut reclaimed_files = HashSet::new();
    for (_, header, record_ref) in write_buffer.iter() {
        flush_stats.num_records += 1;
        if header.is_tombstone() {
            flush_stats.num_tombstone_records += 1;
        }

        flush_stats.data_size += header.page_size();
        if let RecordRef::DeallocPages(pages) = record_ref {
            if header.former_file_id() as u64 != NAN_ID {
                reclaimed_files.insert(header.former_file_id());
            }
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

    flush_stats.num_skip_pages = skip_pages.len();
    flush_stats.num_dealloc_pages = dealloc_pages.len();

    (dealloc_pages, skip_pages, reclaimed_files)
}

fn version_snapshot(version: &Version) -> VersionEdit {
    let new_files: Vec<NewFile> = version.files().values().map(Into::into).collect::<Vec<_>>();

    // FIXME: only the deleted files of the current version are recorded here, and
    // the files of previous versions are not recorded here.
    let deleted_files = version.obsoleted_files();
    VersionEdit {
        new_files,
        deleted_files,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        path::Path,
        sync::Arc,
    };

    use super::{drain_obsoleted_files, FlushCtx};
    use crate::{
        env::Photon,
        page_store::{
            page_file::FileMeta,
            version::{Version, VersionOwner},
            FileInfo, Manifest, PageFiles, WriteBuffer,
        },
        util::shutdown::ShutdownNotifier,
    };

    async fn new_flush_ctx(base: &Path) -> FlushCtx<Photon> {
        std::fs::create_dir_all(base).unwrap();
        let notifier = ShutdownNotifier::default();
        let shutdown = notifier.subscribe();
        let version = Version::new(1 << 16, 1, HashMap::default(), HashSet::new());
        let version_owner = Arc::new(VersionOwner::new(version));
        FlushCtx {
            shutdown,
            version_owner,
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
            let (deleted_pages, file_info, _) = ctx.build_page_file(&wb).await.unwrap();
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
            let (deleted_pages, file_info, _) = ctx.build_page_file(&wb).await.unwrap();
            assert!(deleted_pages.is_empty());
            assert!(!file_info.is_page_active(addr));
        }
    }

    #[test]
    fn obsoleted_files_drain() {
        // It drains all obsoleted files.
        let mut files = HashMap::new();
        files.insert(1, make_obsoleted_file(1));
        files.insert(2, make_active_file(2, 32));
        let obsoleted_files = drain_obsoleted_files(&mut files, HashSet::default());
        assert!(obsoleted_files.contains(&1));
        assert!(!obsoleted_files.contains(&2));
        assert!(files.contains_key(&2));
        assert!(!files.contains_key(&1));
    }

    fn make_obsoleted_file(id: u32) -> FileInfo {
        let active_pages = roaring::RoaringBitmap::new();
        let meta = FileMeta::new(id, 0, Vec::default(), BTreeMap::default(), 4096);
        FileInfo::new(active_pages, 0, id, id, HashSet::default(), Arc::new(meta))
    }

    fn make_active_file(id: u32, page_addr: u32) -> FileInfo {
        let mut active_pages = roaring::RoaringBitmap::new();
        active_pages.insert(page_addr);
        let meta = FileMeta::new(id, 1, Vec::default(), BTreeMap::default(), 4096);
        FileInfo::new(active_pages, 1, id, id, HashSet::default(), Arc::new(meta))
    }

    #[test]
    fn obsoleted_files_skip_refering_files() {
        let mut files = HashMap::new();
        files.insert(1, make_active_file(1, 32));
        files.insert(2, make_obsoleted_file_but_refer_others(2, 1));
        let obsoleted_files = drain_obsoleted_files(&mut files, HashSet::default());
        assert!(obsoleted_files.is_empty());
        assert!(files.contains_key(&1));
        assert!(files.contains_key(&2));
    }

    fn make_obsoleted_file_but_refer_others(id: u32, refer: u32) -> FileInfo {
        let active_pages = roaring::RoaringBitmap::new();
        let meta = FileMeta::new(id, 0, Vec::default(), BTreeMap::default(), 4096);
        let mut refers = HashSet::default();
        refers.insert(refer);
        FileInfo::new(active_pages, 0, id, id, refers, Arc::new(meta))
    }

    #[test]
    fn obsoleted_files_drain_reclaimed() {
        let mut files = HashMap::new();
        files.insert(1, make_obsoleted_file(1));
        files.insert(2, make_obsoleted_file_but_refer_others(2, 1));
        files.insert(3, make_active_file(2, 32));

        let mut reclaimed_files = HashSet::new();
        reclaimed_files.insert(2);
        let obsoleted_files = drain_obsoleted_files(&mut files, reclaimed_files);
        assert!(obsoleted_files.contains(&1));
        assert!(obsoleted_files.contains(&2));
        assert!(!obsoleted_files.contains(&3));
        assert!(!files.contains_key(&1));
        assert!(!files.contains_key(&2));
        assert!(files.contains_key(&3));
    }
}
