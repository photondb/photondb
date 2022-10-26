use std::sync::Arc;

use crate::page_store::{PageFiles, Version};

pub(crate) struct CleanupCtx {
    // TODO: cancel task
    page_files: Arc<PageFiles>,
}

impl CleanupCtx {
    pub(crate) fn new(page_files: Arc<PageFiles>) -> Self {
        CleanupCtx { page_files }
    }

    pub(crate) async fn run(self, mut version: Version) {
        loop {
            let deleted_files = version.deleted_files();

            let mut next_version = version.wait_next_version().await;
            std::mem::swap(&mut next_version, &mut version);
            next_version.wait_version_released().await;

            // Now it is safety to cleanup the version.
            self.clean_obsolated_files(deleted_files).await;
        }
    }

    #[inline]
    async fn clean_obsolated_files(&self, files: Vec<u32>) {
        if let Err(err) = self.page_files.remove_files(files).await {
            todo!("{err}");
        }
    }
}
