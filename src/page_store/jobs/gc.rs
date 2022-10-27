use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;

use crate::page_store::{FileInfo, PageFiles, Result, StrategyBuilder, Version};

/// An abstraction describes how to move pages to the end of page files.
#[async_trait]
pub(crate) trait RewritePage: Send + Sync {
    /// Rewrite the corresponding page to the end of page files.
    async fn rewrite(&self, page_id: u64) -> Result<()>;
}

pub(crate) struct GcCtx {
    // TODO: cancel task
    rewriter: Arc<dyn RewritePage>,
    strategy_builder: Box<dyn StrategyBuilder>,
    #[allow(unused)]
    page_files: Arc<PageFiles>,

    cleaned_files: HashSet<u32>,
}

impl GcCtx {
    pub(crate) fn new(
        rewriter: Arc<dyn RewritePage>,
        strategy_builder: Box<dyn StrategyBuilder>,
        page_files: Arc<PageFiles>,
    ) -> Self {
        GcCtx {
            rewriter,
            strategy_builder,
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
        for (_, file) in version.files() {
            let file_id = file.get_file_id();
            if cleaned_files.contains(&file_id) {
                self.cleaned_files.insert(file_id);
                continue;
            }
            strategy.collect(file);
        }

        while let Some(file_id) = strategy.apply() {
            let file = version.files().get(&file_id).expect("File must exists");
            if let Err(err) = self.forward_active_pages(&file).await {
                todo!("do_recycle: {err:?}");
            }
            self.cleaned_files.insert(file_id);
        }
    }

    async fn forward_active_pages(&self, file: &FileInfo) -> Result<()> {
        for _page_addr in file.iter() {
            // TODO: convert page_addr to page_id.
            let page_id = 0;
            self.rewriter.rewrite(page_id).await?;
        }
        // TODO: rewrite deleted pages.
        Ok(())
    }
}
