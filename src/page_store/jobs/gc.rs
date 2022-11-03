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

    pub(crate) async fn run(mut self, mut version: Version) {
        loop {
            self.gc(&version).await;
            match with_shutdown(&mut self.shutdown, version.wait_next_version()).await {
                Some(next_version) => version = next_version.refresh().unwrap_or(next_version),
                None => break,
            }
        }
    }

    async fn gc(&mut self, version: &Version) {
        let now = version.next_file_id();
        let mut strategy = self.strategy_builder.build(now);
        let cleaned_files = std::mem::take(&mut self.cleaned_files);
        for file in version.files().values() {
            let file_id = file.get_file_id();
            if cleaned_files.contains(&file_id) {
                self.cleaned_files.insert(file_id);
                continue;
            }
            strategy.collect(file);
        }

        while let Some(file_id) = strategy.apply() {
            if self.shutdown.is_terminated() {
                break;
            }

            let file = version.files().get(&file_id).expect("File must exists");
            if let Err(err) = self.rewrite_file(file, version).await {
                todo!("do_recycle: {err:?}");
            }
            self.cleaned_files.insert(file_id);
        }
    }

    async fn rewrite_file(&self, file: &FileInfo, version: &Version) -> Result<()> {
        let file_id = file.get_file_id();
        let reader = self.page_files.open_meta_reader(file_id).await?;
        let page_table = reader.read_page_table().await?;
        let dealloc_pages = reader.read_delete_pages().await?;

        let version = Arc::new(version.clone());
        self.rewrite_active_pages(file, &version, &page_table)
            .await?;
        self.rewrite_dealloc_pages(&version, &dealloc_pages).await?;

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
        version: &Arc<Version>,
        dealloc_pages: &[u64],
    ) -> Result<()> {
        let active_files = version.files();
        let mut cached_pages = Vec::with_capacity(128);
        for page_addr in dealloc_pages {
            let file_id = (page_addr >> 32) as u32;
            if !active_files.contains_key(&file_id) {
                continue;
            }
            cached_pages.push(*page_addr);
            if cached_pages.len() == 128 {
                self.rewrite_dealloc_pages_chunk(version, &cached_pages)?;
                cached_pages.clear();
            }
        }
        if !cached_pages.is_empty() {
            self.rewrite_dealloc_pages_chunk(version, &cached_pages)?;
        }
        Ok(())
    }

    fn rewrite_dealloc_pages_chunk(&self, version: &Arc<Version>, pages: &[u64]) -> Result<()> {
        loop {
            let guard = Guard::new(version.clone(), &self.page_table, &self.page_files);
            let txn = guard.begin();
            match txn.dealloc_pages(pages) {
                Ok(()) => return Ok(()),
                Err(Error::Again) => continue,
                Err(err) => return Err(err),
            }
        }
    }
}
