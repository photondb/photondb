use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::Instant,
};

use log::{info, trace};

use crate::{
    env::Env,
    page_store::{
        page_file::{read_page_table, MapFileBuilder, PartialFileBuilder},
        page_table::PageTable,
        strategy::{PickedFile, ReclaimPickStrategy},
        Error, FileInfo, Guard, MapFileInfo, Options, PageFiles, Result, StrategyBuilder, Version,
    },
    util::shutdown::{with_shutdown, Shutdown},
};

/// Rewrites pages to reclaim disk space.
pub(crate) trait RewritePage<E: Env>: Send + Sync + 'static {
    type Rewrite<'a>: Future<Output = Result<()>> + Send + 'a
    where
        Self: 'a;

    /// Rewrites the corresponding page to reclaim the space it occupied.
    fn rewrite(&self, page_id: u64, guard: Guard<E>) -> Self::Rewrite<'_>;
}

pub(crate) struct ReclaimCtx<E, R>
where
    E: Env,
    R: RewritePage<E>,
{
    options: Options,
    shutdown: Shutdown,

    rewriter: R,
    strategy_builder: Box<dyn StrategyBuilder>,

    page_table: PageTable,
    page_files: Arc<PageFiles<E>>,

    cleaned_files: HashSet<u32>,
}

#[derive(Debug)]
struct ReclaimJobBuilder {
    target_file_base: usize,

    compound_files: HashSet<u32>,
    compound_size: usize,
    compact_files: HashSet<u32>,
    compact_size: usize,
}

#[derive(Debug)]
enum ReclaimJob {
    /// Rewrite page file.
    Rewrite(u32),
    /// Compound a set of page files into a new map file.
    Compound(HashSet<u32>),
    /// Compact a set of map files into a new map file.
    Compact(HashSet<u32>),
}

impl<E, R> ReclaimCtx<E, R>
where
    E: Env,
    R: RewritePage<E>,
{
    pub(crate) fn new(
        options: Options,
        shutdown: Shutdown,
        rewriter: R,
        strategy_builder: Box<dyn StrategyBuilder>,
        page_table: PageTable,
        page_files: Arc<PageFiles<E>>,
    ) -> Self {
        ReclaimCtx {
            options,
            shutdown,
            rewriter,
            strategy_builder,
            page_table,
            page_files,
            cleaned_files: HashSet::default(),
        }
    }

    pub(crate) async fn run(mut self, mut version: Arc<Version>) {
        loop {
            self.reclaim(&version).await;
            match with_shutdown(&mut self.shutdown, version.wait_next_version()).await {
                Some(next_version) => version = next_version.refresh().unwrap_or(next_version),
                None => break,
            }
        }
    }

    async fn reclaim(&mut self, version: &Arc<Version>) {
        // Reclaim deleted files in `cleaned_files`.
        let cleaned_files = std::mem::take(&mut self.cleaned_files);

        // Ignore the strategy, pick and reclaimate empty page files directly.
        let empty_files = self.pick_empty_page_files(version, &cleaned_files);
        self.rewrite_files(empty_files, version).await;

        if !self.is_reclaimable(version, &cleaned_files) {
            return;
        }

        self.reclaim_files_by_strategy(version, &cleaned_files)
            .await;
    }

    fn pick_empty_page_files(
        &mut self,
        version: &Version,
        cleaned_files: &HashSet<u32>,
    ) -> Vec<u32> {
        let mut empty_files = Vec::default();
        for (id, file) in version.page_files() {
            if cleaned_files.contains(id) {
                self.cleaned_files.insert(*id);
                continue;
            }

            if file.is_empty() {
                empty_files.push(*id);
            }
        }
        empty_files
    }

    async fn rewrite_files(&mut self, files: Vec<u32>, version: &Arc<Version>) {
        for file_id in files {
            if self.shutdown.is_terminated() || version.has_next_version() {
                break;
            }

            if let Err(err) = self.rewrite_file(file_id, version).await {
                todo!("rewrite files: {err:?}");
            }
        }
    }

    async fn reclaim_files_by_strategy(
        &mut self,
        version: &Arc<Version>,
        cleaned_files: &HashSet<u32>,
    ) {
        let mut strategy = self.build_strategy(version, cleaned_files);
        let mut builder = ReclaimJobBuilder::new(self.options.file_base_size);
        while let Some((file, active_size)) = strategy.apply() {
            if let Some(job) = builder.add(file, active_size) {
                match job {
                    ReclaimJob::Rewrite(file_id) => {
                        if let Err(err) = self.rewrite_file(file_id, version).await {
                            todo!("reclaim files: {err:?}");
                        }
                    }
                    ReclaimJob::Compound(victims) => {
                        self.reclaim_page_files(version, victims).await;
                    }
                    ReclaimJob::Compact(_map_files) => {
                        todo!()
                    }
                }
            }

            if self.shutdown.is_terminated()
                || version.has_next_version()
                || !self.is_reclaimable(version, &self.cleaned_files)
            {
                break;
            }
        }
    }

    /// Reclaim page files by compounding victims into a new map file, and
    /// install new version.
    async fn reclaim_page_files(&self, version: &Arc<Version>, victims: HashSet<u32>) {
        // TODO: alloc map file id.
        let new_map_file_id = 1;
        let file_infos = version.page_files();
        self.compound_page_files(new_map_file_id, file_infos, victims)
            .await
            .unwrap();

        // TODO: install new version.
    }

    async fn rewrite_file(&mut self, file_id: u32, version: &Arc<Version>) -> Result<()> {
        if self.cleaned_files.contains(&file_id) {
            // This file has been rewritten.
            return Ok(());
        }

        let file = version
            .page_files()
            .get(&file_id)
            .expect("File must exists");
        self.rewrite_file_impl(file, version).await?;
        self.cleaned_files.insert(file_id);
        Ok(())
    }

    async fn rewrite_file_impl(&self, file: &FileInfo, version: &Arc<Version>) -> Result<()> {
        let start_at = Instant::now();
        let file_id = file.get_file_id();
        let reader = self.page_files.open_page_file_meta_reader(file_id).await?;
        let page_table = reader.read_page_table().await?;
        let dealloc_pages = reader.read_delete_pages().await?;

        let total_rewrite_pages = self
            .rewrite_active_pages(file, version, &page_table)
            .await?;
        let total_dealloc_pages = self
            .rewrite_dealloc_pages(file_id, version, &dealloc_pages)
            .await?;

        let effective_size = file.effective_size();
        let file_size = file.file_size();
        let free_size = file_size - effective_size;
        let free_ratio = free_size as f64 / file_size as f64;
        let elapsed = start_at.elapsed().as_micros();
        info!(
            "Rewrite file {file_id} with {total_rewrite_pages} active pages, \
                {total_dealloc_pages} dealloc pages, relocate {effective_size} bytes, \
                free {free_size} bytes, free ratio {free_ratio:.4}, latest {elapsed} microseconds",
        );

        Ok(())
    }

    async fn rewrite_active_pages(
        &self,
        file: &FileInfo,
        version: &Arc<Version>,
        page_table: &BTreeMap<u64, u64>,
    ) -> Result<usize> {
        let mut total_rewrite_pages = 0;
        let mut rewrite_pages = HashSet::new();
        for page_addr in file.iter() {
            let page_id = page_table
                .get(&page_addr)
                .cloned()
                .expect("Page mapping must exists in page table");
            total_rewrite_pages += 1;
            if rewrite_pages.contains(&page_id) {
                continue;
            }
            rewrite_pages.insert(page_id);
            let guard = Guard::new(
                version.clone(),
                self.page_table.clone(),
                self.page_files.clone(),
            );
            self.rewriter.rewrite(page_id, guard).await?;
        }
        Ok(total_rewrite_pages)
    }

    async fn rewrite_dealloc_pages(
        &self,
        file_id: u32,
        version: &Arc<Version>,
        dealloc_pages: &[u64],
    ) -> Result<usize> {
        let active_files = version.page_files();
        let mut total_rewrite_pages = 0;
        let mut cached_pages = Vec::with_capacity(128);
        for page_addr in dealloc_pages {
            let file_id = (page_addr >> 32) as u32;
            if !active_files.contains_key(&file_id) {
                continue;
            }

            if cached_pages.len() == 128 {
                self.rewrite_dealloc_pages_chunk(None, version, &cached_pages)
                    .await?;
                cached_pages.clear();
            }
            cached_pages.push(*page_addr);
            total_rewrite_pages += 1;
        }

        // Ensure the `file_id` is recorded in write buffer.
        if total_rewrite_pages != 0 {
            assert!(!cached_pages.is_empty());
            self.rewrite_dealloc_pages_chunk(Some(file_id), version, &cached_pages)
                .await?;
        }

        Ok(total_rewrite_pages)
    }

    async fn rewrite_dealloc_pages_chunk(
        &self,
        file_id: Option<u32>,
        version: &Arc<Version>,
        pages: &[u64],
    ) -> Result<()> {
        loop {
            let guard = Guard::new(
                version.clone(),
                self.page_table.clone(),
                self.page_files.clone(),
            );
            let txn = guard.begin().await;
            match txn.dealloc_pages(file_id, pages).await {
                Ok(()) => return Ok(()),
                Err(Error::Again) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    fn build_strategy(
        &mut self,
        version: &Version,
        cleaned_files: &HashSet<u32>,
    ) -> Box<dyn ReclaimPickStrategy> {
        let files = version.page_files();
        let now = files.keys().cloned().max().unwrap_or(1);
        let mut strategy = self.strategy_builder.build(now);
        for (id, file) in files {
            if cleaned_files.contains(id) {
                self.cleaned_files.insert(*id);
                continue;
            }

            if !file.is_empty() {
                strategy.collect_page_file(file);
            }
        }
        strategy
    }

    fn is_reclaimable(&self, version: &Version, cleaned_files: &HashSet<u32>) -> bool {
        let used_space = compute_used_space(version.page_files(), cleaned_files);
        let base_size = compute_base_size(version.page_files(), cleaned_files);
        let additional_size = used_space.saturating_sub(base_size);
        let target_space_amp = self.options.max_space_amplification_percent as u64;

        // For log
        let space_amp = (additional_size as f64) / (base_size as f64);

        // Recalculate `space_used_high`, and allow a amount of free space when the base
        // data size exceeds the threshold.
        let space_used_high = std::cmp::max(
            self.options.space_used_high,
            (base_size as f64 * 1.1) as u64,
        );
        if space_used_high < used_space {
            trace!(
                "db is reclaimable: space used {} exceeds water mark {}, base size {}, amp {:.4}",
                used_space,
                self.options.space_used_high,
                base_size,
                space_amp
            );
            true
        } else if 0 < additional_size && target_space_amp * base_size <= additional_size * 100 {
            trace!(
                "db is reclaimable: space amplification {:.4} exceeds target {}, base size {}, used space {}",
                space_amp, target_space_amp, base_size, used_space
            );
            true
        } else {
            trace!(
                "db is not reclaimable, base size {}, additional size {}, space amp {:.4}",
                base_size,
                additional_size,
                space_amp
            );
            false
        }
    }

    /// Compound a set of page files into a map file.
    ///
    /// We don't add `victims` into `ReclaimCtx::cleaned_files`, because there
    /// might exists dealloc pages.
    ///
    /// NOTE: We don't mix page file and map file in compounding, because they
    /// have different age (update frequency).
    #[allow(unused)]
    async fn compound_page_files(
        &self,
        new_file_id: u32,
        file_infos: &HashMap<u32, FileInfo>,
        victims: HashSet<u32>,
    ) -> Result<(HashMap<u32, FileInfo>, MapFileInfo)> {
        let mut builder = self.page_files.new_map_file_builder(new_file_id).await?;
        for id in victims {
            let file_builder = builder.add_file(id);
            let file_info = file_infos.get(&id).expect("Victims must exists");
            let page_file = self
                .page_files
                .open_page_reader(id, file_info.meta().block_size())
                .await?;
            builder = self
                .compound_partial_page_file(file_builder, file_info)
                .await?;
        }
        builder.finish().await
    }

    /// Write all active pages of the corresponding page file into a map file
    /// (not include dealloc pages).
    async fn compound_partial_page_file<'a>(
        &self,
        mut builder: PartialFileBuilder<'a, E>,
        file_info: &FileInfo,
    ) -> Result<MapFileBuilder<'a, E>> {
        let file_id = file_info.get_file_id();
        let file_meta = file_info.meta();
        let block_size = file_meta.block_size();
        let reader = self
            .page_files
            .open_page_reader(file_id, block_size)
            .await?;
        let page_table = read_page_table(&reader, &file_meta).await?;
        let mut page = vec![];
        for page_addr in file_info.iter() {
            let page_id = page_table
                .get(&page_addr)
                .cloned()
                .expect("Page mapping must exists in page table");
            let handle = file_info
                .get_page_handle(page_addr)
                .expect("Handle of active page must exists");
            let page_size = handle.size as usize;
            if page.len() < page_size {
                page.resize(page_size, 0u8);
            }
            reader
                .read_exact_at(&mut page[..page_size], handle.offset as u64)
                .await?;
            builder
                .add_page(page_id, page_addr, &page[..page_size])
                .await?;
        }
        builder.finish().await
    }

    /// Compact a set of map files into a new map file, and release mark the
    /// compacted files as obsoleted to reclaim space.
    #[allow(unused)]
    async fn compact_map_files(
        &self,
        new_file_id: u32,
        map_files: &HashMap<u32, MapFileInfo>,
        victims: HashSet<u32>,
    ) -> Result<(HashMap<u32, FileInfo>, MapFileInfo)> {
        todo!()
    }
}

impl ReclaimJobBuilder {
    fn new(target_file_base: usize) -> ReclaimJobBuilder {
        ReclaimJobBuilder {
            target_file_base,

            compact_files: HashSet::default(),
            compact_size: 0,
            compound_files: HashSet::default(),
            compound_size: 0,
        }
    }

    fn add(&mut self, file: PickedFile, active_size: usize) -> Option<ReclaimJob> {
        // A switch disable map files before we have full supports.
        if true {
            let PickedFile::PageFile(file_id) = file else { panic!("not implemented") };
            return Some(ReclaimJob::Rewrite(file_id));
        }

        match file {
            PickedFile::PageFile(file_id) => {
                // Rewrite small page files or hot pages directly.
                if active_size < 16 << 10 || self.is_top_k(file_id) {
                    return Some(ReclaimJob::Rewrite(file_id));
                }

                self.compound_size += active_size;
                self.compound_files.insert(file_id);
                if self.compound_size >= self.target_file_base {
                    self.compound_size = 0;
                    return Some(ReclaimJob::Compound(std::mem::take(
                        &mut self.compound_files,
                    )));
                }
            }
            PickedFile::MapFile(file_id) => {
                // TODO: rewrite small map file directly.
                self.compact_size += active_size;
                self.compact_files.insert(file_id);
                if self.compact_size >= self.target_file_base {
                    self.compact_size = 0;
                    return Some(ReclaimJob::Compact(std::mem::take(&mut self.compact_files)));
                }
            }
        }
        None
    }

    fn is_top_k(&self, _file_id: u32) -> bool {
        todo!()
    }
}

fn compute_base_size(files: &HashMap<u32, FileInfo>, cleaned_files: &HashSet<u32>) -> u64 {
    // skip files that are already being cleaned.
    let allow_file =
        |info: &&FileInfo| !info.is_empty() && !cleaned_files.contains(&info.get_file_id());
    files
        .values()
        .filter(allow_file)
        .map(FileInfo::effective_size)
        .sum::<usize>() as u64
}

fn compute_used_space(files: &HashMap<u32, FileInfo>, cleaned_files: &HashSet<u32>) -> u64 {
    // skip files that are already being cleaned.
    let allow_file =
        |info: &&FileInfo| !info.is_empty() && !cleaned_files.contains(&info.get_file_id());
    files
        .values()
        .filter(allow_file)
        .map(FileInfo::file_size)
        .sum::<usize>() as u64
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        path::Path,
        sync::Mutex,
    };

    use tempdir::TempDir;

    use super::*;
    use crate::{
        env::Photon,
        page_store::{version::DeltaVersion, MinDeclineRateStrategyBuilder, RecordRef},
        util::shutdown::ShutdownNotifier,
    };

    #[derive(Clone, Default)]
    struct PageRewriter {
        values: Arc<Mutex<Vec<u64>>>,
    }

    impl PageRewriter {
        fn pages(&self) -> Vec<u64> {
            self.values.lock().unwrap().clone()
        }
    }

    impl RewritePage<Photon> for PageRewriter {
        type Rewrite<'a> = impl Future<Output = Result<(), Error>> + Send + 'a
        where
            Self: 'a;

        fn rewrite(&self, id: u64, _guard: Guard<Photon>) -> Self::Rewrite<'_> {
            self.values.lock().unwrap().push(id);
            async { Ok(()) }
        }
    }

    async fn build_page_file(
        page_files: &PageFiles<Photon>,
        file_id: u32,
        pages: &[(u64, u64)],
        dealloc_pages: &[u64],
    ) -> FileInfo {
        let mut builder = page_files.new_page_file_builder(file_id).await.unwrap();
        for (page_id, page_addr) in pages {
            builder.add_page(*page_id, *page_addr, &[0]).await.unwrap();
        }
        builder.add_delete_pages(dealloc_pages);
        builder.finish().await.unwrap()
    }

    async fn build_reclaim_ctx(
        dir: &Path,
        rewriter: PageRewriter,
    ) -> ReclaimCtx<Photon, PageRewriter> {
        let notifier = ShutdownNotifier::new();
        let shutdown = notifier.subscribe();
        let strategy_builder = Box::new(MinDeclineRateStrategyBuilder);
        let options = Options {
            cache_capacity: 2 << 10,
            ..Default::default()
        };
        let page_files = Arc::new(PageFiles::new(Photon, dir, &options).await);
        ReclaimCtx {
            options,
            shutdown,
            rewriter,
            strategy_builder,
            page_table: PageTable::default(),
            page_files,
            cleaned_files: HashSet::default(),
        }
    }

    #[photonio::test]
    async fn reclaim_rewrite_page() {
        let root = TempDir::new("reclaim_rewrite_page").unwrap();
        let root = root.into_path();

        let rewriter = PageRewriter::default();
        let ctx = build_reclaim_ctx(&root, rewriter.clone()).await;
        let mut file_info = build_page_file(
            &ctx.page_files,
            2,
            &[
                (1, pa(2, 16)),
                (2, pa(2, 32)),
                (3, pa(2, 64)),
                (4, pa(2, 128)),
            ],
            &[301, 302, 303],
        )
        .await;
        file_info.deactivate_page(3, 32);

        let mut files = HashMap::new();
        files.insert(2, file_info.clone());
        let delta = DeltaVersion {
            page_files: files,
            ..Default::default()
        };
        let version = Arc::new(Version::new(1 << 20, 3, 8, delta));

        ctx.rewrite_file_impl(&file_info, &version).await.unwrap();
        assert_eq!(rewriter.pages(), vec![1, 3, 4]); // page_id 2 is deallocated.

        let buf = version.min_write_buffer();
        buf.seal().unwrap();
        let dealloc_pages = HashSet::from([301, 302, 303]);
        for (_, header, record_ref) in buf.iter() {
            match record_ref {
                RecordRef::DeallocPages(pages) => {
                    assert_eq!(header.former_file_id(), 2);
                    for page in pages {
                        assert!(dealloc_pages.contains(&page));
                    }
                }
                RecordRef::Page(_page) => unreachable!(),
            }
        }
    }

    fn pa(file_id: u32, offset: u32) -> u64 {
        ((file_id as u64) << 32) | (offset as u64)
    }

    #[photonio::test]
    async fn compound_page_files() {
        let root = TempDir::new("compound_page_files").unwrap();
        let root = root.into_path();

        let rewriter = PageRewriter::default();
        let ctx = build_reclaim_ctx(&root, rewriter.clone()).await;
        let file_id_1 = 2;
        let file_id_2 = 3;
        let mut file_info_1 = build_page_file(
            &ctx.page_files,
            file_id_1,
            &[
                (1, pa(file_id_1, 16)),
                (2, pa(file_id_1, 32)),
                (3, pa(file_id_1, 64)),
                (4, pa(file_id_1, 128)),
            ],
            &[301, 302, 303],
        )
        .await;
        file_info_1.deactivate_page(3, pa(file_id_1, 32));

        let file_info_2 = build_page_file(
            &ctx.page_files,
            file_id_2,
            &[
                (11, pa(file_id_2, 16)),
                (12, pa(file_id_2, 32)),
                (13, pa(file_id_2, 64)),
                (14, pa(file_id_2, 128)),
            ],
            &[301, 302, 303],
        )
        .await;

        let mut files = HashMap::new();
        files.insert(file_id_1, file_info_1.clone());
        files.insert(file_id_2, file_info_2.clone());

        let victims = files.keys().cloned().collect::<HashSet<_>>();
        let (new_files, map_file) = ctx.compound_page_files(1, &files, victims).await.unwrap();
        assert_eq!(map_file.meta().num_page_files(), 2);
        assert!(new_files.contains_key(&file_id_1));
        assert!(new_files.contains_key(&file_id_2));
        assert!(new_files
            .get(&file_id_1)
            .unwrap()
            .is_page_active(pa(file_id_1, 16)));
        assert!(!new_files
            .get(&file_id_1)
            .unwrap()
            .is_page_active(pa(file_id_1, 32)));
        assert!(new_files
            .get(&file_id_2)
            .unwrap()
            .is_page_active(pa(file_id_2, 32)));
    }
}
