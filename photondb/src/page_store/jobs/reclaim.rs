use std::{sync::Arc, time::Instant};

use log::{debug, error, info, trace};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    env::Env,
    page::PageRef,
    page_store::{
        page_file::{FileBuilder, FileMetaHolder, FileReader, PageGroupBuilder},
        stats::AtomicJobStats,
        strategy::ReclaimPickStrategy,
        version::{DeltaVersion, VersionOwner, VersionUpdateReason},
        FileInfo, Manifest, NewFile, Options, PageFiles, PageGroup, Result, StrategyBuilder,
        StreamEdit, Version, VersionEdit,
    },
    util::shutdown::{with_shutdown, Shutdown},
};

pub(crate) struct ReclaimCtx<E>
where
    E: Env,
{
    options: Options,
    shutdown: Shutdown,

    strategy_builder: Box<dyn StrategyBuilder>,

    page_files: Arc<PageFiles<E>>,
    version_owner: Arc<VersionOwner>,
    manifest: Arc<futures::lock::Mutex<Manifest<E>>>,

    cleaned_files: FxHashSet<u32>,

    job_stats: Arc<AtomicJobStats>,
}

#[derive(Debug)]
struct ReclaimJobBuilder {
    target_file_base: usize,

    compact_files: FxHashSet<u32>,
    compact_size: usize,
}

#[derive(Debug)]
enum ReclaimJob {
    /// Compact a set of files into a new file.
    Compact(FxHashSet<u32>),
}

#[derive(Debug, Default)]
struct ReclaimProgress {
    // some options.
    target_space_amp: u64,
    space_used_high: u64,
    file_base_size: u64,

    used_space: u64,
    base_size: u64,
    additional_size: u64,
}

#[derive(Debug)]
enum ReclaimReason {
    None,
    HighSpaceUsage,
    LargeSpaceAmp,
}

#[derive(Debug, Default)]
struct CompactStats {
    num_active_pages: usize,
    num_dealloc_pages: usize,
    input_size: usize,
    output_size: usize,
}

impl<E> ReclaimCtx<E>
where
    E: Env,
{
    // FIXME: reduce number of arguments
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        options: Options,
        shutdown: Shutdown,
        strategy_builder: Box<dyn StrategyBuilder>,
        page_files: Arc<PageFiles<E>>,
        version_owner: Arc<VersionOwner>,
        manifest: Arc<futures::lock::Mutex<Manifest<E>>>,
        job_stats: Arc<AtomicJobStats>,
    ) -> Self {
        ReclaimCtx {
            options,
            shutdown,
            strategy_builder,
            page_files,
            version_owner,
            manifest,
            cleaned_files: FxHashSet::default(),
            job_stats,
        }
    }

    pub(crate) async fn run(mut self, mut version: Arc<Version>) {
        loop {
            if !self.options.disable_space_reclaiming {
                self.reclaim(&version).await;
                version.reclaimed();
            }
            match with_shutdown(&mut self.shutdown, version.wait_next_version()).await {
                Some(next_version) => version = next_version.refresh().unwrap_or(next_version),
                None => break,
            }
        }
    }

    async fn reclaim(&mut self, version: &Arc<Version>) {
        // Reclaim deleted files in `cleaned_files`.
        let cleaned_files = std::mem::take(&mut self.cleaned_files);
        let mut progress = ReclaimProgress::new(&self.options, version, &cleaned_files);
        progress.trace_log();
        if !progress.is_reclaimable() {
            return;
        }

        if let Err(err) = self
            .reclaim_files_by_strategy(&mut progress, version, &cleaned_files)
            .await
        {
            error!("Reclaim files: {err:?}");
        }
    }

    async fn reclaim_files_by_strategy(
        &mut self,
        progress: &mut ReclaimProgress,
        version: &Arc<Version>,
        cleaned_files: &FxHashSet<u32>,
    ) -> Result<()> {
        let now = {
            let lock = self.manifest.lock().await;
            lock.now()
        };
        let mut strategy = self.build_strategy(now, version, cleaned_files);
        let mut builder = ReclaimJobBuilder::new(self.options.file_base_size);
        while let Some((file, active_size)) = strategy.apply() {
            if let Some(job) = builder.add(file, active_size) {
                match job {
                    ReclaimJob::Compact(victims) => {
                        self.reclaim_files(progress, version, victims).await?;
                    }
                }
            }

            if self.shutdown.is_terminated()
                || version.has_next_version()
                || !progress.is_reclaimable()
            {
                break;
            }
        }
        Ok(())
    }

    async fn reclaim_files(
        &mut self,
        progress: &mut ReclaimProgress,
        version: &Arc<Version>,
        victims: FxHashSet<u32>,
    ) -> Result<()> {
        let file_id = {
            let mut lock = self.manifest.lock().await;
            lock.next_file_id()
        };

        let file_infos = version.file_infos();
        let page_groups = version.page_groups();
        let (page_groups, file_info) = self
            .compact_files(progress, file_id, file_infos, page_groups, &victims)
            .await?;

        // All input are obsoleted, since it doesn't relocate pages.
        let edit = make_compact_version_edit(&file_info, &victims);
        let mut manifest = self.manifest.lock().await;
        let version = self.version_owner.current();
        manifest
            .record_version_edit(edit, || super::version_snapshot(&version))
            .await?;

        let mut delta = DeltaVersion::from(version.as_ref());
        delta.reason = VersionUpdateReason::Compact;
        delta.file_infos.retain(|id, _| !victims.contains(id));
        delta.file_infos.insert(file_id, file_info);
        // FIXME: need remove empty infos if it is not contained in.
        delta.page_groups.extend(page_groups.into_iter());
        delta.obsoleted_files = victims.into_iter().collect();
        // Safety: the mutable reference of [`Manifest`] is hold.
        unsafe { self.version_owner.install(delta) };
        Ok(())
    }

    fn build_strategy(
        &mut self,
        now: u32,
        version: &Version,
        cleaned_files: &FxHashSet<u32>,
    ) -> Box<dyn ReclaimPickStrategy> {
        let mut strategy = self.strategy_builder.build(now);
        let page_groups = version.page_groups();
        let file_infos = version.file_infos();
        for (&id, file) in file_infos {
            if cleaned_files.contains(&id) {
                self.cleaned_files.insert(id);
                continue;
            }

            strategy.collect_file(page_groups, file);
        }
        strategy
    }

    /// Compact a set of files into a new file, and release mark the compacted
    /// files as obsoleted to reclaim space.
    async fn compact_files(
        &mut self,
        progress: &mut ReclaimProgress,
        new_file_id: u32,
        file_infos: &FxHashMap<u32, FileInfo>,
        page_groups: &FxHashMap<u32, PageGroup>,
        victims: &FxHashSet<u32>,
    ) -> Result<(FxHashMap<u32, PageGroup>, FileInfo)> {
        let start_at = Instant::now();
        let mut builder = self
            .page_files
            .new_file_builder(
                new_file_id,
                self.options.compression_on_cold_compact,
                self.options.page_checksum_type,
            )
            .await?;
        let mut victims = victims.iter().cloned().collect::<Vec<_>>();
        victims.sort_unstable();
        let mut stats = CompactStats::default();
        let mut up2_sum = 0;
        for &id in &victims {
            let info = file_infos.get(&id).expect("Victim must exists");
            up2_sum += info.up2();
            builder = self
                .compact_file(builder, &mut stats, info, page_groups)
                .await?;
            self.cleaned_files.insert(id);
            progress.track_file(info, page_groups);
        }

        // When we include the page in a new segment that contains re-written pages from
        // other segments, the value for up2 for the new segment is the average up2 for
        // all pages written to it.
        let up2 = up2_sum / (victims.len() as u32);
        let (page_groups, file_info) = builder.finish(up2).await?;

        let elapsed = start_at.elapsed().as_micros();
        let CompactStats {
            num_active_pages,
            num_dealloc_pages,
            input_size,
            output_size,
        } = stats;
        self.job_stats.compact_input_bytes.add(input_size as u64);
        self.job_stats.compact_write_bytes.add(output_size as u64);
        let free_size = input_size.saturating_sub(output_size);
        let free_ratio = (free_size as f64) / (input_size as f64);
        info!(
            "Compact files {victims:?} into a new file {new_file_id} \
                    with up2 {up2}, relocate {num_active_pages} pages, \
                    dealloc {num_dealloc_pages} pages, \
                    relocate {output_size} bytes, \
                    free {free_size} bytes, free ratio {free_ratio:.4}, \
                    latest {elapsed} microseconds"
        );

        Ok((page_groups, file_info))
    }

    async fn compact_file<'a>(
        &self,
        mut builder: FileBuilder<'a, E>,
        stats: &mut CompactStats,
        file_info: &FileInfo,
        page_groups: &FxHashMap<u32, PageGroup>,
    ) -> Result<FileBuilder<'a, E>> {
        let file_id = file_info.meta().file_id;
        let existed_groups = file_info
            .meta()
            .referenced_groups
            .iter()
            .filter(|g| page_groups.contains_key(g))
            .cloned()
            .collect::<FxHashSet<_>>();

        let mut file_meta = self.page_files.read_file_meta(file_id).await?;
        file_meta
            .dealloc_pages
            .retain(|&addr| existed_groups.contains(&((addr >> 32) as u32)));
        builder.add_dealloc_pages(&file_meta.dealloc_pages);
        stats.num_dealloc_pages += file_meta.dealloc_pages.len();

        debug!(
            "compact file {file_id}, {} dealloc pages, {} page groups",
            file_meta.dealloc_pages.len(),
            file_meta.page_groups.len()
        );

        let reader = self
            .page_files
            .open_page_reader(file_id, file_info.meta().block_size)
            .await?;
        let mut target_groups = file_meta.page_groups.keys().cloned().collect::<Vec<_>>();
        target_groups.sort_unstable();
        for id in target_groups {
            let Some(page_group) = page_groups.get(&id) else {
                debug!("Skip forwarding page group {id} of file {file_id}, because it is obsoleted");
                continue;
            };

            assert!(!page_group.is_empty());
            let mut group_builder = builder.add_page_group(id);
            self.compact_page_group(
                &mut group_builder,
                stats,
                &reader,
                file_info,
                &file_meta,
                page_group,
            )
            .await?;
            builder = group_builder.finish().await?;
        }
        self.job_stats
            .read_file_bytes
            .add(reader.total_read_bytes());
        Ok(builder)
    }

    async fn compact_page_group<'a>(
        &self,
        builder: &mut PageGroupBuilder<'a, E>,
        stats: &mut CompactStats,
        reader: &FileReader<<E as Env>::PositionalReader>,
        file_info: &FileInfo,
        file_meta: &FileMetaHolder,
        page_group: &PageGroup,
    ) -> Result<()> {
        let group_id = page_group.meta().group_id;
        stats.collect(page_group);

        let page_table = file_meta.page_tables.get(&group_id).expect("Must exists");
        let mut page = vec![];
        for page_addr in page_group.iter() {
            let handle = page_group.get_page_handle(page_addr).expect("Must exists");
            let page_size = handle.size as usize;
            if page.len() < page_size {
                page.resize(page_size, 0u8);
            }
            page.truncate(page_size);
            self.page_files
                .read_file_page_from_reader(reader, file_info.meta(), handle, &mut page)
                .await?;
            let page_id = *page_table.get(&page_addr).expect("Must exists");
            let page_ref = PageRef::new(page.as_slice());
            builder
                .add_page(page_id, page_addr, page_ref.info(), &page)
                .await?;
        }
        Ok(())
    }
}

impl ReclaimJobBuilder {
    fn new(target_file_base: usize) -> ReclaimJobBuilder {
        ReclaimJobBuilder {
            target_file_base,

            compact_files: FxHashSet::default(),
            compact_size: 0,
        }
    }

    fn add(&mut self, file_id: u32, active_size: usize) -> Option<ReclaimJob> {
        self.compact_size += active_size;
        self.compact_files.insert(file_id);
        if self.compact_size >= self.target_file_base {
            self.compact_size = 0;
            return Some(ReclaimJob::Compact(std::mem::take(&mut self.compact_files)));
        }
        None
    }
}

impl ReclaimProgress {
    fn new(option: &Options, version: &Version, cleaned_files: &FxHashSet<u32>) -> ReclaimProgress {
        let target_space_amp = option.max_space_amplification_percent as u64;
        let space_used_high = option.space_used_high;
        let file_base_size = option.file_base_size as u64;
        let used_space = compute_used_space(version.file_infos(), cleaned_files);
        let base_size = compute_base_size(version.page_groups(), cleaned_files);
        let additional_size = used_space.saturating_sub(base_size);
        ReclaimProgress {
            target_space_amp,
            space_used_high,
            file_base_size,
            used_space,
            base_size,
            additional_size,
        }
    }

    fn track_file(&mut self, file: &FileInfo, page_files: &FxHashMap<u32, PageGroup>) {
        self.used_space = self.used_space.saturating_sub(file.meta().file_size as u64);
        let effective_size = file
            .meta()
            .page_groups
            .keys()
            .map(|id| {
                page_files
                    .get(id)
                    .map(PageGroup::effective_size)
                    .unwrap_or_default()
            })
            .sum::<usize>() as u64;
        self.base_size = self.base_size.saturating_sub(effective_size);
        self.additional_size = self.used_space.saturating_sub(self.base_size);
    }

    fn reclaim_reason(&self) -> ReclaimReason {
        // If space usage exceeds high watermark,
        if self.space_used_high < self.used_space
            // .. and enough space for reclaiming.
            && 2 * self.file_base_size < self.additional_size
        {
            ReclaimReason::HighSpaceUsage
        } else if 0 < self.additional_size
            && self.target_space_amp * self.base_size <= self.additional_size * 100
        {
            ReclaimReason::LargeSpaceAmp
        } else {
            ReclaimReason::None
        }
    }

    fn is_reclaimable(&self) -> bool {
        match self.reclaim_reason() {
            ReclaimReason::HighSpaceUsage | ReclaimReason::LargeSpaceAmp => true,
            ReclaimReason::None => false,
        }
    }

    fn trace_log(&self) {
        let space_amp = (self.additional_size as f64) / (self.base_size as f64);
        match self.reclaim_reason() {
            ReclaimReason::HighSpaceUsage => {
                trace!(
                    "db is reclaimable: space used {} exceeds water mark {}, base size {}, amp {:.4}",
                    self.used_space,
                    self.space_used_high,
                    self.base_size,
                    space_amp
                );
            }
            ReclaimReason::LargeSpaceAmp => {
                trace!(
                    "db is reclaimable: space amplification {:.4} exceeds target {}, base size {}, used space {}",
                    space_amp, self.target_space_amp, self.base_size, self.used_space
                );
            }
            ReclaimReason::None => {
                trace!(
                    "db is not reclaimable, base size {}, additional size {}, used space {}, used high {}, space amp {:.4}",
                    self.base_size,
                    self.additional_size,
                    self.used_space,
                    self.space_used_high,
                    space_amp
                );
            }
        }
    }
}

impl CompactStats {
    fn collect(&mut self, page_group: &PageGroup) {
        self.num_active_pages += page_group.num_active_pages();
        self.input_size += page_group.meta().total_page_size();
        self.output_size += page_group.effective_size();
    }
}

/// Wait until the running reclaiming progress to finish.
pub(crate) async fn wait_for_reclaiming(options: &Options, mut version: Arc<Version>) {
    if options.disable_space_reclaiming {
        return;
    }

    loop {
        let progress = ReclaimProgress::new(options, &version, &FxHashSet::default());
        progress.trace_log();
        if progress.is_reclaimable() {
            version.wait_for_reclaiming().await;
            if let Some(next_version) = version.try_next() {
                version = next_version;
                continue;
            }
        }
        break;
    }
}

fn compute_base_size(
    page_files: &FxHashMap<u32, PageGroup>,
    cleaned_files: &FxHashSet<u32>,
) -> u64 {
    // skip files that are already being cleaned.
    let allow_file =
        |info: &&PageGroup| !info.is_empty() && !cleaned_files.contains(&info.meta().file_id);
    page_files
        .values()
        .filter(allow_file)
        .map(PageGroup::effective_size)
        .sum::<usize>() as u64
}

fn compute_used_space(
    file_infos: &FxHashMap<u32, FileInfo>,
    cleaned_files: &FxHashSet<u32>,
) -> u64 {
    file_infos
        .values()
        .filter(|info| !cleaned_files.contains(&info.meta().file_id))
        .map(|info| info.meta().file_size)
        .sum::<usize>() as u64
}

fn make_compact_version_edit(
    file_info: &FileInfo,
    obsoleted_files: &FxHashSet<u32>,
) -> VersionEdit {
    let deleted_files = obsoleted_files.iter().cloned().collect::<Vec<_>>();
    let new_files = vec![NewFile::from(file_info)];
    VersionEdit {
        file_stream: Some(StreamEdit {
            new_files,
            deleted_files,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path};

    use tempdir::TempDir;

    use super::*;
    use crate::{
        env::Photon,
        page::PageInfo,
        page_store::{
            page_file::Compression, version::DeltaVersion, ChecksumType,
            MinDeclineRateStrategyBuilder,
        },
        util::shutdown::ShutdownNotifier,
    };

    async fn build_reclaim_ctx(dir: &Path) -> ReclaimCtx<Photon> {
        let notifier = ShutdownNotifier::new();
        let shutdown = notifier.subscribe();
        let strategy_builder = Box::new(MinDeclineRateStrategyBuilder);
        let options = Options {
            cache_capacity: 2 << 10,
            ..Default::default()
        };
        let manifest = Arc::new(futures::lock::Mutex::new(
            Manifest::open(Photon, &dir).await.unwrap(),
        ));
        let version_owner = Arc::new(VersionOwner::new(Version::new(
            1 << 20,
            1,
            10,
            DeltaVersion::default(),
        )));
        let page_files = Arc::new(PageFiles::new(Photon, dir, &options).await.unwrap());
        ReclaimCtx {
            options,
            shutdown,
            strategy_builder,
            page_files,
            manifest,
            version_owner,
            cleaned_files: HashSet::default(),
            job_stats: Arc::default(),
        }
    }

    fn pa(file_id: u32, offset: u32) -> u64 {
        ((file_id as u64) << 32) | (offset as u64)
    }

    async fn build_file(
        page_files: &PageFiles<Photon>,
        file_id: u32,
        pages: FxHashMap<u32, Vec<(u64, u64)>>,
    ) -> (FxHashMap<u32, PageGroup>, FileInfo) {
        let mut builder = page_files
            .new_file_builder(file_id, Compression::ZSTD, ChecksumType::CRC32)
            .await
            .unwrap();
        for (id, pages) in pages {
            let mut file_builder = builder.add_page_group(id);
            for (page_id, page_addr) in pages {
                let page_info = PageInfo::from_raw(0, 0, 32);
                file_builder
                    .add_page(page_id, page_addr, page_info, &[0; 32])
                    .await
                    .unwrap();
            }
            builder = file_builder.finish().await.unwrap();
        }
        builder.finish(file_id).await.unwrap()
    }

    #[photonio::test]
    async fn files_compacting() {
        let root = TempDir::new("compact_files").unwrap();
        let root = root.into_path();

        let mut ctx = build_reclaim_ctx(&root).await;

        let (f1, f2, f3, f4) = (1, 2, 3, 4);
        let (m1, m2, m3) = (1, 2, 3);
        let mut pages = FxHashMap::default();
        pages.insert(f1, vec![(1, pa(f1, 16)), (2, pa(f1, 32)), (3, pa(f1, 64))]);
        pages.insert(f2, vec![(4, pa(f2, 16)), (5, pa(f2, 32)), (6, pa(f2, 64))]);
        let (virtual_infos, m1_info) = build_file(&ctx.page_files, m1, pages).await;
        let mut page_files = virtual_infos;

        let mut pages = FxHashMap::default();
        pages.insert(f3, vec![(7, pa(f3, 16)), (8, pa(f3, 32)), (9, pa(f3, 64))]);
        pages.insert(f4, vec![(1, pa(f4, 16)), (2, pa(f4, 32)), (3, pa(f4, 64))]);
        let (virtual_infos, m2_info) = build_file(&ctx.page_files, m2, pages).await;
        page_files.extend(virtual_infos.into_iter());

        let mut map_files = FxHashMap::default();
        map_files.insert(m1, m1_info);
        map_files.insert(m2, m2_info);
        let victims = HashSet::from_iter(vec![m1, m2].into_iter());
        let version = ctx.version_owner.current();
        let mut progress = ReclaimProgress::new(&ctx.options, &version, &HashSet::default());
        let (virtual_infos, m3_info) = ctx
            .compact_files(&mut progress, m3, &map_files, &page_files, &victims)
            .await
            .unwrap();

        assert!(virtual_infos.contains_key(&f1));
        assert!(virtual_infos.contains_key(&f2));
        assert!(virtual_infos.contains_key(&f3));
        assert!(virtual_infos.contains_key(&f4));

        let f1_info = virtual_infos.get(&f1).unwrap();
        assert!(f1_info.get_page_handle(pa(f1, 32)).is_some());
        assert!(f1_info.get_page_handle(pa(f1, 64)).is_some());
        assert!(f1_info.get_page_handle(pa(f1, 128)).is_none());

        let f4_info = virtual_infos.get(&f4).unwrap();
        assert!(f4_info.get_page_handle(pa(f4, 0)).is_none());
        assert!(f4_info.get_page_handle(pa(f4, 32)).is_some());
        assert!(f4_info.get_page_handle(pa(f4, 64)).is_some());

        let base_size = virtual_infos
            .values()
            .map(|info| info.effective_size())
            .sum::<usize>();
        let used_size = m3_info.meta().file_size;
        println!("base size {base_size}");
        println!("used size {used_size}");
        assert!(base_size < used_size);
    }

    #[photonio::test]
    async fn files_reclaiming() {
        let root = TempDir::new("map_files_reclaiming").unwrap();
        let root = root.into_path();

        let mut ctx = build_reclaim_ctx(&root).await;

        let (f1, f2, f3, f4) = (1, 2, 3, 4);
        let (m1, m2, m3) = (1, 2, 3);
        {
            let mut lock = ctx.manifest.lock().await;
            lock.reset_next_file_id(m3);
        }
        let mut pages = FxHashMap::default();
        pages.insert(f1, vec![(1, pa(f1, 16)), (2, pa(f1, 32)), (3, pa(f1, 64))]);
        pages.insert(f2, vec![(4, pa(f2, 16)), (5, pa(f2, 32)), (6, pa(f2, 64))]);
        let (virtual_infos, m1_info) = build_file(&ctx.page_files, m1, pages).await;
        let mut page_groups = virtual_infos;

        let mut pages = FxHashMap::default();
        pages.insert(f3, vec![(7, pa(f3, 16)), (8, pa(f3, 32)), (9, pa(f3, 64))]);
        pages.insert(f4, vec![(1, pa(f4, 16)), (2, pa(f4, 32)), (3, pa(f4, 64))]);
        let (virtual_infos, m2_info) = build_file(&ctx.page_files, m2, pages).await;
        page_groups.extend(virtual_infos.into_iter());

        let mut file_infos = FxHashMap::default();
        file_infos.insert(m1, m1_info);
        file_infos.insert(m2, m2_info);
        let victims = HashSet::from_iter(vec![m1, m2].into_iter());

        let delta = DeltaVersion {
            reason: VersionUpdateReason::Flush,
            page_groups,
            file_infos,
            ..Default::default()
        };
        // No concurrent operations.
        unsafe { ctx.version_owner.install(delta) };
        let version = ctx.version_owner.current();
        let mut progress = ReclaimProgress::new(&ctx.options, &version, &HashSet::default());
        ctx.reclaim_files(&mut progress, &version, victims)
            .await
            .unwrap();

        let version = ctx.version_owner.current();
        let page_groups = version.page_groups();
        assert!(page_groups.contains_key(&f1));
        assert!(page_groups.contains_key(&f2));
        assert!(page_groups.contains_key(&f3));
        assert!(page_groups.contains_key(&f4));

        let f1_info = page_groups.get(&f1).unwrap();
        assert!(f1_info.get_page_handle(pa(f1, 32)).is_some());
        assert!(f1_info.get_page_handle(pa(f1, 64)).is_some());
        assert!(f1_info.get_page_handle(pa(f1, 128)).is_none());

        let f4_info = page_groups.get(&f4).unwrap();
        assert!(f4_info.get_page_handle(pa(f4, 0)).is_none());
        assert!(f4_info.get_page_handle(pa(f4, 32)).is_some());
        assert!(f4_info.get_page_handle(pa(f4, 64)).is_some());

        let map_files = version.file_infos();
        // The compacted map files are not contained in version.
        assert!(!map_files.contains_key(&m1));
        assert!(!map_files.contains_key(&m2));
        assert!(map_files.contains_key(&m3));
    }
}
