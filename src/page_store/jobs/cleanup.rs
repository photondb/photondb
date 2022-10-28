use std::sync::Arc;

use crate::{
    page_store::{PageFiles, Version},
    util::shutdown::{with_shutdown, Shutdown},
};

pub(crate) struct CleanupCtx {
    shutdown: Shutdown,
    page_files: Arc<PageFiles>,
}

impl CleanupCtx {
    pub(crate) fn new(shutdown: Shutdown, page_files: Arc<PageFiles>) -> Self {
        CleanupCtx {
            shutdown,
            page_files,
        }
    }

    pub(crate) async fn run(mut self, mut version: Version) {
        loop {
            let deleted_files = version.deleted_files();

            let Some(mut next_version) = with_shutdown(
                &mut self.shutdown, version.wait_next_version()).await else {
                break;
            };

            std::mem::swap(&mut next_version, &mut version);
            if with_shutdown(&mut self.shutdown, next_version.wait_version_released())
                .await
                .is_none()
            {
                break;
            }

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
