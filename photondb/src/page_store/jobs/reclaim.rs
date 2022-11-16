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
        page_table::PageTable, strategy::ReclaimPickStrategy, Error, FileInfo, Guard, Options,
        PageFiles, Result, StrategyBuilder, Version,
    },
    util::shutdown::{with_shutdown, Shutdown},
};

/// Rewrites pages to reclaim disk space.
pub(crate) trait RewritePage<E: Env>: Send + Sync + 'static {
    type Rewrite<'a>: Future<Output = Result<()>> + Send + 'a
    where
        Self: 'a;

    /// Rewrites the corresponding page to reclaim the space it occupied.
    fn rewrite<'a>(&'a self, page_id: u64, guard: Guard<'a, E>) -> Self::Rewrite<'a>;
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
        for (id, file) in version.files() {
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
        while let Some(file_id) = strategy.apply() {
            if let Err(err) = self.rewrite_file(file_id, version).await {
                todo!("reclaim files: {err:?}");
            }

            if self.shutdown.is_terminated()
                || version.has_next_version()
                || !self.is_reclaimable(version, &self.cleaned_files)
            {
                break;
            }
        }
    }

    async fn rewrite_file(&mut self, file_id: u32, version: &Arc<Version>) -> Result<()> {
        if self.cleaned_files.contains(&file_id) {
            // This file has been rewritten.
            return Ok(());
        }

        let file = version.files().get(&file_id).expect("File must exists");
        self.rewrite_file_impl(file, version).await?;
        self.cleaned_files.insert(file_id);
        Ok(())
    }

    async fn rewrite_file_impl(&self, file: &FileInfo, version: &Arc<Version>) -> Result<()> {
        let start_at = Instant::now();
        let file_id = file.get_file_id();
        let reader = self.page_files.open_meta_reader(file_id).await?;
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
            let guard = Guard::new(version.clone(), &self.page_table, &self.page_files);
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
        let active_files = version.files();
        let mut total_rewrite_pages = 0;
        let mut cached_pages = Vec::with_capacity(128);
        for page_addr in dealloc_pages {
            let file_id = (page_addr >> 32) as u32;
            if !active_files.contains_key(&file_id) {
                continue;
            }

            if cached_pages.len() == 128 {
                self.rewrite_dealloc_pages_chunk(None, version, &cached_pages)?;
                cached_pages.clear();
            }
            cached_pages.push(*page_addr);
            total_rewrite_pages += 1;
        }

        // Ensure the `file_id` is recorded in write buffer.
        if total_rewrite_pages != 0 {
            assert!(!cached_pages.is_empty());
            self.rewrite_dealloc_pages_chunk(Some(file_id), version, &cached_pages)?;
        }

        Ok(total_rewrite_pages)
    }

    fn rewrite_dealloc_pages_chunk(
        &self,
        file_id: Option<u32>,
        version: &Arc<Version>,
        pages: &[u64],
    ) -> Result<()> {
        loop {
            let guard = Guard::new(version.clone(), &self.page_table, &self.page_files);
            let txn = guard.begin();
            match txn.dealloc_pages(file_id, pages) {
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
        let files = version.files();
        let now = files.keys().cloned().max().unwrap_or(1);
        let mut strategy = self.strategy_builder.build(now);
        for (id, file) in files {
            if cleaned_files.contains(id) {
                self.cleaned_files.insert(*id);
                continue;
            }

            if !file.is_empty() {
                strategy.collect(file);
            }
        }
        strategy
    }

    fn is_reclaimable(&self, version: &Version, cleaned_files: &HashSet<u32>) -> bool {
        let used_space = compute_used_space(version.files(), cleaned_files);
        let base_size = compute_base_size(version.files(), cleaned_files);
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
        page_store::{MinDeclineRateStrategyBuilder, RecordRef},
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

        fn rewrite<'a>(&'a self, id: u64, _guard: Guard<'a, Photon>) -> Self::Rewrite<'a> {
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
        let mut builder = page_files.new_file_builder(file_id).await.unwrap();
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
        let options = Options::default();
        ReclaimCtx {
            options,
            shutdown,
            rewriter,
            strategy_builder,
            page_table: PageTable::default(),
            page_files: Arc::new(PageFiles::new(Photon, dir, false).await),
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
        let version = Arc::new(Version::new(1 << 20, 3, files, HashSet::default()));

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
}
