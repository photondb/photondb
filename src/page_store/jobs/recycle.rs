use std::sync::{Arc, Weak};

use async_trait::async_trait;

use crate::{
    env::Env,
    page_store::{FileInfo, PageStore, Result, Version},
};

/// An abstraction describes how to forward pages to the end of page files.
#[async_trait(?Send)]
pub(crate) trait ForwardPage {
    /// Forward the corresponding page to the end of page files.
    async fn forward(&self, page_addr: u64) -> Result<()>;
}

/// An abstraction describes the strategy of page files recycle.
pub(crate) trait RecyclePickStrategy {
    /// Returns recycle threshold of this strategy.
    fn threshold(&self) -> f64;

    /// Compute and return score of the corresponding page file.
    fn score(&self, file_info: &FileInfo) -> f64;
}

pub(crate) struct RecycleCtx<E: Env> {
    // TODO: cancel task
    forward: Weak<dyn ForwardPage>,
    strategy: Box<dyn RecyclePickStrategy>,
    page_store: Arc<PageStore<E>>,
}

impl<E: Env> RecycleCtx<E> {
    pub async fn run(self, mut version: Version) {
        loop {
            self.do_recycle(&version).await;
            version = version.wait_next_version().await;
        }
    }

    async fn do_recycle(&self, version: &Version) {
        for (_, file) in version.files() {
            if !self.should_recycle_file(file) {
                continue;
            }
            if let Err(err) = self.forward_active_pages(&file).await {
                todo!("do_recycle: {err:?}");
            }
        }
    }

    fn should_recycle_file(&self, file: &FileInfo) -> bool {
        self.strategy.score(file) >= self.strategy.threshold()
    }

    async fn forward_active_pages(&self, file: &FileInfo) -> Result<()> {
        let Some(forward) = self.forward.upgrade() else {
            return Ok(())
        };

        for page_addr in file.iter() {
            forward.forward(page_addr).await?;
        }
        Ok(())
    }
}
