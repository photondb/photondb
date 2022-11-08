use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    sync::Arc,
    time::Instant,
};

use log::info;

use crate::{
    env::Env,
    page_store::{
        page_table::PageTable, Error, FileInfo, Guard, PageFiles, Result, StrategyBuilder, Version,
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

pub(crate) struct GcCtx<E, R>
where
    E: Env,
    R: RewritePage<E>,
{
    shutdown: Shutdown,

    rewriter: R,
    strategy_builder: Box<dyn StrategyBuilder>,

    page_table: PageTable,
    page_files: Arc<PageFiles<E>>,

    cleaned_files: HashSet<u32>,
}

impl<E, R> GcCtx<E, R>
where
    E: Env,
    R: RewritePage<E>,
{
    pub(crate) fn new(
        shutdown: Shutdown,
        rewriter: R,
        strategy_builder: Box<dyn StrategyBuilder>,
        page_table: PageTable,
        page_files: Arc<PageFiles<E>>,
    ) -> Self {
        GcCtx {
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
            self.gc(&version).await;
            match with_shutdown(&mut self.shutdown, version.wait_next_version()).await {
                Some(next_version) => version = next_version.refresh().unwrap_or(next_version),
                None => break,
            }
        }
    }

    async fn gc(&mut self, version: &Arc<Version>) {
        // Reclaim deleted files in `cleaned_files`.
        let cleaned_files = std::mem::take(&mut self.cleaned_files);

        // Ignore the strategy, pick and reclaimate empty page files directly.
        let empty_files = self.pick_empty_page_files(version, &cleaned_files);
        self.rewrite_files(empty_files, version).await;

        let picked_files = self.pick_page_file_by_strategy(version, &cleaned_files);
        self.rewrite_files(picked_files, version).await;
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

    fn pick_page_file_by_strategy(
        &mut self,
        version: &Version,
        cleaned_files: &HashSet<u32>,
    ) -> Vec<u32> {
        let now = version.next_file_id();
        let mut strategy = self.strategy_builder.build(now);
        for (id, file) in version.files() {
            if cleaned_files.contains(id) {
                self.cleaned_files.insert(*id);
                continue;
            }

            strategy.collect(file);
        }

        let mut files = Vec::default();
        while let Some(file_id) = strategy.apply() {
            files.push(file_id);
        }
        files
    }

    async fn rewrite_files(&mut self, files: Vec<u32>, version: &Arc<Version>) {
        for file_id in files {
            if self.shutdown.is_terminated() {
                break;
            }

            if self.cleaned_files.contains(&file_id) {
                // This file has been rewritten.
                continue;
            }

            let file = version.files().get(&file_id).expect("File must exists");
            if let Err(err) = self.rewrite_file(file, version).await {
                todo!("do_recycle: {err:?}");
            }
            self.cleaned_files.insert(file_id);
        }
    }

    async fn rewrite_file(&self, file: &FileInfo, version: &Arc<Version>) -> Result<()> {
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

        info!(
            "Rewrite file {file_id} with {} active pages, {} dealloc pages, latest {} microseconds",
            total_rewrite_pages,
            total_dealloc_pages,
            start_at.elapsed().as_micros()
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

    async fn build_gc_ctx(dir: &Path, rewriter: PageRewriter) -> GcCtx<Photon, PageRewriter> {
        let notifier = ShutdownNotifier::new();
        let shutdown = notifier.subscribe();
        let strategy_builder = Box::new(MinDeclineRateStrategyBuilder::new(1 << 30, usize::MAX));
        GcCtx {
            shutdown,
            rewriter,
            strategy_builder,
            page_table: PageTable::default(),
            page_files: Arc::new(PageFiles::new(Photon, dir, "db").await),
            cleaned_files: HashSet::default(),
        }
    }

    #[photonio::test]
    async fn gc_rewrite_page() {
        let root = TempDir::new("gc_rewrite_page").unwrap();
        let root = root.into_path();

        let rewriter = PageRewriter::default();
        let ctx = build_gc_ctx(&root, rewriter.clone()).await;
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

        ctx.rewrite_file(&file_info, &version).await.unwrap();
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
