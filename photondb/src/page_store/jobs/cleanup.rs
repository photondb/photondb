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

            let files = next_version.obsoleted_files();
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
            self.clean_obsoleted_files(&version, files).await;
        }
    }

    #[inline]
    async fn clean_obsoleted_files(&self, version: &Version, obsoleted_files: Vec<u32>) {
        {
            let file_infos = version.file_infos();
            for &id in &obsoleted_files {
                if file_infos.contains_key(&id) {
                    panic!("A obsoleted file {id} still exists in a visible version");
                }
            }
        }

        if !obsoleted_files.is_empty() {
            info!("Clean obsoleted files {obsoleted_files:?}");
            self.page_files.evict_cached_pages(&obsoleted_files);
            self.page_files.remove_files(obsoleted_files).await;
        }
    }
}
