use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    sync::Arc,
};

use crate::{
    env::Env,
    page_store::{
        page_table::PageTable, Error, FileInfo, Guard, PageFiles, Result, StrategyBuilder, Version,
    },
    util::shutdown::{with_shutdown, Shutdown},
};

/// Rewrites pages to reclaim disk space.
pub(crate) trait RewritePageChain<E: Env>: Send + Sync + 'static {
    type Rewrite<'a>: Future<Output = Result<()>> + Send + 'a
    where
        Self: 'a;

    /// Rewrites the corresponding page to reclaim the space it occupied.
    fn rewrite<'a>(&'a self, node_id: u64, guard: Guard<'a, E>) -> Self::Rewrite<'a>;
}

pub(crate) struct GcCtx<E, R>
where
    E: Env,
    R: RewritePageChain<E>,
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
    R: RewritePageChain<E>,
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
        let file_id = file.get_file_id();
        let reader = self.page_files.open_meta_reader(file_id).await?;
        let page_table = reader.read_page_table().await?;
        let dealloc_pages = reader.read_delete_pages().await?;

        self.rewrite_active_pages(file, version, &page_table)
            .await?;
        self.rewrite_dealloc_pages(file_id, version, &dealloc_pages)
            .await?;

        Ok(())
    }

    async fn rewrite_active_pages(
        &self,
        file: &FileInfo,
        version: &Arc<Version>,
        page_table: &BTreeMap<u64, u64>,
    ) -> Result<()> {
        let mut rewrite_pages = HashSet::new();
        for page_addr in file.iter() {
            let page_id = page_table
                .get(&page_addr)
                .cloned()
                .expect("Page mapping must exists in page table");
            if rewrite_pages.contains(&page_id) {
                continue;
            }
            rewrite_pages.insert(page_id);
            let guard = Guard::new(version.clone(), &self.page_table, &self.page_files);
            self.rewriter.rewrite(page_id, guard).await?;
        }
        Ok(())
    }

    async fn rewrite_dealloc_pages(
        &self,
        file_id: u32,
        version: &Arc<Version>,
        dealloc_pages: &[u64],
    ) -> Result<()> {
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

        Ok(())
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
