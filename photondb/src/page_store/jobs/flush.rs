use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use log::info;

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

        self.save_and_install_version(file_id, file_info, dealloc_pages, reclaimed_files, wait)
            .await?;

        write_buffer.on_flushed();

        Ok(())
    }

    async fn save_and_install_version(
        &self,
        file_id: u32,
        file_info: FileInfo,
        dealloc_pages: Vec<u64>,
        reclaimed_files: HashSet<u32>,
        wait_new_buffer: bool,
    ) -> Result<()> {
        let mut manifest = self.manifest.lock().await;
        let version = self.version_owner.current();

        let (mut files, map_files) =
            self.apply_dealloc_pages(&version, file_id, dealloc_pages.to_owned());
        let obsoleted_page_files = drain_obsoleted_files(&mut files, reclaimed_files);
        files.insert(file_id, file_info);

        let edit = make_flush_version_edit(file_id, &obsoleted_page_files);
        manifest
            .record_version_edit(edit, || version_snapshot(&version))
            .await?;

        // Release buffer permit and ensure the new buffer is installed, before install
        // new version.
        if wait_new_buffer {
            version.buffer_set.release_permit_and_wait(file_id).await;
        }

        let delta = DeltaVersion {
            reason: VersionUpdateReason::Flush,
            page_files: files,
            map_files,
            obsoleted_page_files,
            ..DeltaVersion::from(version.as_ref())
        };
        // Safety: the mutable reference of [`Manifest`] is hold.
        unsafe { self.version_owner.install(delta) };
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

        self.page_files.evict_cached_pages(&dealloc_pages);

        let mut builder = self
            .page_files
            .new_page_file_builder(file_id, self.options.compression_on_flush)
            .await?;
        let mut write_bytes = 0;
        let mut discard_bytes = 0;
        for (page_addr, header, record_ref) in write_buffer.iter() {
            match record_ref {
                RecordRef::DeallocPages(_) => {}
                RecordRef::Page(page) => {
                    if header.is_tombstone() || skip_pages.contains(&page_addr) {
                        discard_bytes += header.page_size();
                        continue;
                    }
                    let content = page.data();
                    builder
                        .add_page(header.page_id(), page_addr, content)
                        .await?;
                    write_bytes += content.len();
                    let _ = self.page_files.populate_cache(page_addr, content);
                }
            }
        }
        builder.add_delete_pages(&dealloc_pages);
        let file_info = builder.finish().await?;

        self.job_stats.flush_write_bytes.add(write_bytes as u64);
        self.job_stats.flush_discard_bytes.add(discard_bytes as u64);

        Ok((dealloc_pages, file_info, reclaimed_files))
    }

    fn apply_dealloc_pages(
        &self,
        version: &Version,
        now: u32,
        dealloc_pages: Vec<u64>,
    ) -> (HashMap<u32, FileInfo>, HashMap<u32, MapFileInfo>) {
        let mut files = version.page_files().clone();
        let mut map_files = version.map_files().clone();
        for page_addr in dealloc_pages {
            let file_id = (page_addr >> 32) as u32;
            if let Some(file_info) = files.get_mut(&file_id) {
                if !file_info.deactivate_page(now, page_addr) {
                    continue;
                }
                if let Some(map_file_id) = file_info.get_map_file_id() {
                    let map_file = map_files.get_mut(&map_file_id).expect("Must exists");
                    map_file.on_update(now);
                }
            };
        }
        (files, map_files)
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
        .iter()
        // Virtual file could ignore reclaimed info.
        .filter(|(id, info)| info.get_map_file_id().is_some() || !reclaimed_files.contains(id))
        .map(|(&id, _)| id)
        .collect::<HashSet<_>>();
    let mut obsoleted_files = reclaimed_files;
    // Some reclaimed files has been cleaned by normal dealloc page operations.
    obsoleted_files.retain(|id| files.contains_key(id));
    for (id, info) in &mut *files {
        if info.get_map_file_id().is_none() && info.is_obsoleted(&active_files) {
            obsoleted_files.insert(*id);
        }
    }
    // Ignore virtual file.
    files.drain_filter(|id, info| info.get_map_file_id().is_none() && obsoleted_files.contains(id));
    obsoleted_files
}

fn assert_files_are_deletable(files: &HashMap<u32, FileInfo>, reclaimed_files: &HashSet<u32>) {
    for file_id in reclaimed_files {
        if let Some(info) = files.get(file_id) {
            if !info.is_empty() && info.get_map_file_id().is_none() {
                panic!("The reclaimed page file {file_id} has active pages");
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
                assert!(reclaimed_files.insert(header.former_file_id()));
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

pub(super) fn version_snapshot(version: &Version) -> VersionEdit {
    let new_files: Vec<NewFile> = version
        .page_files()
        .values()
        .filter(|info| info.get_map_file_id().is_none())
        .map(Into::into)
        .collect::<Vec<_>>();

    // NOTE: Only the deleted files of the current version are recorded here, and
    // the files of previous versions are not recorded here.
    let deleted_files = version.obsoleted_page_files();
    let page_stream = StreamEdit {
        new_files,
        deleted_files,
    };

    let new_files: Vec<NewFile> = version
        .map_files()
        .values()
        .map(Into::into)
        .collect::<Vec<_>>();

    // NOTE: Only the deleted files of the current version are recorded here, and
    // the files of previous versions are not recorded here.
    let deleted_files = version.obsoleted_map_files();
    let map_stream = StreamEdit {
        new_files,
        deleted_files,
    };
    VersionEdit {
        page_stream: Some(page_stream),
        map_stream: Some(map_stream),
    }
}

fn make_flush_version_edit(file_id: u32, obsoleted_files: &HashSet<u32>) -> VersionEdit {
    let deleted_files = obsoleted_files.iter().cloned().collect();
    let new_files = vec![NewFile::from(file_id)];
    let stream = StreamEdit {
        new_files,
        deleted_files,
    };
    VersionEdit {
        page_stream: Some(stream),
        map_stream: None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        path::Path,
        sync::Arc,
    };

    use tempdir::TempDir;

    use super::{drain_obsoleted_files, FlushCtx};
    use crate::{
        env::Photon,
        page_store::{
            page_file::{Compression, FileMeta},
            version::{DeltaVersion, Version, VersionOwner},
            ChecksumType, FileInfo, Manifest, PageFiles, WriteBuffer,
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
            assert!(file_info.may_page_active(addr));
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
            assert!(file_info.may_page_active(addr));
            assert!(file_info.get_page_handle(addr).is_none());
        }
    }

    #[test]
    fn flush_obsoleted_files_drain() {
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
        let meta = FileMeta::new(
            id,
            0,
            4096,
            Vec::default(),
            BTreeMap::default(),
            Compression::NONE,
            ChecksumType::CRC32,
        );
        FileInfo::new(
            0,
            HashSet::new(),
            0,
            id,
            id,
            HashSet::default(),
            Arc::new(meta),
        )
    }

    fn make_active_file(id: u32, _page_addr: u32) -> FileInfo {
        let meta = FileMeta::new(
            id,
            1,
            4096,
            Vec::default(),
            BTreeMap::default(),
            Compression::NONE,
            ChecksumType::CRC32,
        );
        FileInfo::new(
            1,
            HashSet::new(),
            1,
            id,
            id,
            HashSet::default(),
            Arc::new(meta),
        )
    }

    #[test]
    fn flush_obsoleted_files_skip_refering_files() {
        let mut files = HashMap::new();
        files.insert(1, make_active_file(1, 32));
        files.insert(2, make_obsoleted_file_but_refer_others(2, 1));
        let obsoleted_files = drain_obsoleted_files(&mut files, HashSet::default());
        assert!(obsoleted_files.is_empty());
        assert!(files.contains_key(&1));
        assert!(files.contains_key(&2));
    }

    fn make_obsoleted_file_but_refer_others(id: u32, refer: u32) -> FileInfo {
        let meta = FileMeta::new(
            id,
            0,
            4096,
            Vec::default(),
            BTreeMap::default(),
            Compression::NONE,
            ChecksumType::CRC32,
        );
        let mut refers = HashSet::default();
        refers.insert(refer);
        FileInfo::new(0, HashSet::new(), 0, id, id, refers, Arc::new(meta))
    }

    #[test]
    fn flush_obsoleted_files_drain_reclaimed() {
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

    #[photonio::test]
    async fn flush_dealloc_pages_rewritten_pointer_obsoleted_file() {
        let root = TempDir::new("flush_apply_dealloc_pages").unwrap();
        let ctx = new_flush_ctx(root.path()).await;

        // Insert two pages in current buffer.
        let (addr1, addr2, file_1) = {
            let version = ctx.version_owner.current();
            let buf = version.min_write_buffer();
            let (addr1, _, _) = unsafe { buf.alloc_page(1, 32, false) }.unwrap();
            let (addr2, _, _) = unsafe { buf.alloc_page(2, 32, false) }.unwrap();
            seal_and_install_new_buffer(&version, &buf);
            ctx.flush(&buf).await.unwrap();
            (addr1, addr2, buf.file_id())
        };
        println!("Alloc two pages {addr1} {addr2} at file {file_1}");

        // Dealloc page with addr1, install new buffer.
        let file_2 = {
            let version = ctx.version_owner.current();
            let buf = version.min_write_buffer();
            unsafe { buf.dealloc_pages(&[addr1], false) }.unwrap();
            seal_and_install_new_buffer(&version, &buf);
            ctx.flush(&buf).await.unwrap();
            buf.file_id()
        };
        println!("Dealloc page {addr1} at file {file_2}");

        // Dealloc page with addr2, install new buffer without flush.
        let buffer_2 = {
            let version = ctx.version_owner.current();
            let buf = version.min_write_buffer();
            unsafe { buf.dealloc_pages(&[addr2], false) }.unwrap();
            seal_and_install_new_buffer(&version, &buf);
            buf.file_id()
        };
        println!("Dealloc page {addr2} at file {buffer_2}");

        // Rewrite file 2, likes reclaiming, and install new buffer without flush.
        let buffer_3 = {
            let version = ctx.version_owner.current();
            let buffer_3 = version.buffer_set.current().next_buffer_id() - 1;
            let buf = version.get(buffer_3).unwrap(); // in last buffer.
            let header = unsafe { buf.dealloc_pages(&[addr1], false) }.unwrap();
            header.set_former_file_id(file_1);
            seal_and_install_new_buffer(&version, &buf);
            buffer_3
        };
        println!("Rewrite file {file_2}, so it will re-dealloc page {addr1}");

        // Flush buffer_2, so file_1 is empty.
        {
            let version = ctx.version_owner.current();
            let buf = version.get(buffer_2).unwrap().clone();
            ctx.flush(&buf).await.unwrap();
        }
        println!("Flush write buffer {buffer_2}, it make file {file_1} become empty");

        // Flush buffer_3, it will dealloc pages 2 but file_1 is already cleaned.
        {
            let version = ctx.version_owner.current();
            let buf = version.get(buffer_3).unwrap().clone();
            ctx.flush(&buf).await.unwrap();
        }

        // file_1 must not contained in obsoleted files.
        {
            let version = ctx.version_owner.current();
            let obsoleted_files = version.obsoleted_page_files();
            assert!(!obsoleted_files.contains(&file_1));
        }
    }

    fn seal_and_install_new_buffer(version: &Version, buf: &WriteBuffer) {
        buf.seal().unwrap();
        let file_id = buf.file_id() + 1;
        let buf = WriteBuffer::with_capacity(file_id, 1 << 16);
        version.buffer_set.install(Arc::new(buf));
    }
}
