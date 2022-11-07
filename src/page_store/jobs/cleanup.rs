use std::sync::Arc;

use log::info;

use crate::{
    env::Env,
    page_store::{PageFiles, Version},
    util::shutdown::{with_shutdown, Shutdown},
};

pub(crate) struct CleanupCtx<E: Env> {
    shutdown: Shutdown,
    page_files: Arc<PageFiles<E>>,
}

impl<E: Env> CleanupCtx<E> {
    pub(crate) fn new(shutdown: Shutdown, page_files: Arc<PageFiles<E>>) -> Self {
        CleanupCtx {
            shutdown,
            page_files,
        }
    }

    pub(crate) async fn run(mut self, mut version: Arc<Version>) {
        loop {
            let Some(mut next_version) = with_shutdown(
                &mut self.shutdown, version.wait_next_version()).await else {
                break;
            };

            let files = next_version.obsolated_files();
            std::mem::swap(&mut next_version, &mut version);
            if with_shutdown(&mut self.shutdown, next_version.wait_version_released())
                .await
                .is_none()
            {
                break;
            }

            // Now it is safety to cleanup the version.
            self.clean_obsolated_files(files).await;
        }
    }

    #[inline]
    async fn clean_obsolated_files(&self, files: Vec<u32>) {
        if files.is_empty() {
            return;
        }

        info!("Clean obsolated page files {files:?}");
        if let Err(err) = self.page_files.remove_files(files).await {
            todo!("{err}");
        }
    }
}
