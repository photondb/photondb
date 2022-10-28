use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;

use crate::page_store::{
    page_table::PageTable, Error, FileInfo, Guard, PageFiles, Result, StrategyBuilder, Version,
};

/// An abstraction describes how to move pages to the end of page files.
#[async_trait]
pub(crate) trait RewritePage: Send + Sync {
    /// Rewrite the corresponding page to the end of page files.
    async fn rewrite(&self, page_id: u64) -> Result<()>;
}

pub(crate) struct GcCtx {
    // TODO: cancel task
    rewriter: Box<dyn RewritePage>,
    strategy_builder: Box<dyn StrategyBuilder>,

    page_table: PageTable,
    page_files: Arc<PageFiles>,

    cleaned_files: HashSet<u32>,
}

impl GcCtx {
    pub(crate) fn new(
        rewriter: Box<dyn RewritePage>,
        strategy_builder: Box<dyn StrategyBuilder>,
        page_table: PageTable,
        page_files: Arc<PageFiles>,
    ) -> Self {
        GcCtx {
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
            version = version.wait_next_version().await;
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

        self.rewrite_active_pages(file, &page_table).await?;
        self.rewrite_dealloc_pages(version, &dealloc_pages).await?;

        Ok(())
    }

    async fn rewrite_active_pages(
        &self,
        file: &FileInfo,
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
            self.rewriter.rewrite(page_id).await?;
        }
        Ok(())
    }

    async fn rewrite_dealloc_pages(&self, version: &Version, dealloc_pages: &[u64]) -> Result<()> {
        let version = Arc::new(version.clone());

        let active_files = version.files();
        let mut cached_pages = Vec::with_capacity(128);
        for page_addr in dealloc_pages {
            let file_id = (page_addr >> 32) as u32;
            if !active_files.contains_key(&file_id) {
                continue;
            }
            cached_pages.push(*page_addr);
            if cached_pages.len() == 128 {
                self.rewrite_dealloc_pages_chunk(version.clone(), &cached_pages)?;
                cached_pages.clear();
            }
        }
        if !cached_pages.is_empty() {
            self.rewrite_dealloc_pages_chunk(version.clone(), &cached_pages)?;
        }
        Ok(())
    }

    fn rewrite_dealloc_pages_chunk(&self, version: Arc<Version>, pages: &[u64]) -> Result<()> {
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
