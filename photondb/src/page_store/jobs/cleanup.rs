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

            let page_files = next_version.obsoleted_page_files();
            let map_files = next_version.obsoleted_map_files();
            std::mem::swap(&mut next_version, &mut version);
            if with_shutdown(&mut self.shutdown, next_version.wait_version_released())
                .await
                .is_none()
            {
                break;
            }

            // Since all previous versions are invisible, releasing the former buffer is
            // safety.
            version.release_previous_buffers();

            // Now it is safety to cleanup the version.
            self.clean_obsoleted_files(page_files, map_files).await;
        }
    }

    #[inline]
    async fn clean_obsoleted_files(&self, page_files: Vec<u32>, map_files: Vec<u32>) {
        if !page_files.is_empty() {
            info!("Clean obsoleted page files {page_files:?}");
            if let Err(err) = self.page_files.remove_page_files(page_files).await {
                todo!("{err}");
            }
        }
        if !map_files.is_empty() {
            info!("Clean obsoleted map files {map_files:?}");
            if let Err(err) = self.page_files.remove_map_files(map_files).await {
                todo!("{err}");
            }
        }
    }
}
