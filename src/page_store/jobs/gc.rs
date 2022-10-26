use std::sync::Arc;

use async_trait::async_trait;

use crate::page_store::{FileInfo, PageFiles, Result, Version};

/// An abstraction describes how to move pages to the end of page files.
#[async_trait]
pub(crate) trait RewritePage: Send + Sync {
    /// Rewrite the corresponding page to the end of page files.
    async fn rewrite(&self, page_id: u64) -> Result<()>;
}

/// An abstraction describes the strategy of page files gc.
pub(crate) trait GcPickStrategy: Send + Sync {
    /// Returns recycle threshold of this strategy.
    fn threshold(&self) -> f64;

    /// Compute and return score of the corresponding page file.
    fn score(&self, file_info: &FileInfo) -> f64;
}

pub(crate) struct GcCtx {
    // TODO: cancel task
    rewriter: Arc<dyn RewritePage>,
    strategy: Box<dyn GcPickStrategy>,
    #[allow(unused)]
    page_files: Arc<PageFiles>,
}

impl GcCtx {
    pub(crate) fn new(
        rewriter: Arc<dyn RewritePage>,
        strategy: Box<dyn GcPickStrategy>,
        page_files: Arc<PageFiles>,
    ) -> Self {
        GcCtx {
            rewriter,
            strategy,
            page_files,
        }
    }

    pub(crate) async fn run(self, mut version: Version) {
        loop {
            self.gc(&version).await;
            version = version.wait_next_version().await;
        }
    }

    async fn gc(&self, version: &Version) {
        for (_, file) in version.files() {
            if !self.is_satisfied(file) {
                continue;
            }
            if let Err(err) = self.forward_active_pages(&file).await {
                todo!("do_recycle: {err:?}");
            }
        }
    }

    fn is_satisfied(&self, file: &FileInfo) -> bool {
        self.strategy.score(file) >= self.strategy.threshold()
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
