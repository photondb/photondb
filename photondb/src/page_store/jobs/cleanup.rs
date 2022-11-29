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
            self.clean_obsoleted_files(&version, page_files, map_files)
                .await;
        }
    }

    #[inline]
    async fn clean_obsoleted_files(
        &self,
        version: &Version,
        obsoleted_page_files: Vec<u32>,
        obsoleted_map_files: Vec<u32>,
    ) {
        {
            let page_files = version.page_files();
            for &id in &obsoleted_page_files {
                if page_files
                    .get(&id)
                    .map(|info| info.get_map_file_id().is_none())
                    .unwrap_or_default()
                {
                    panic!("A obsoleted page file {id} still exists in a visible version");
                }
            }
            let map_files = version.map_files();
            for &id in &obsoleted_map_files {
                if map_files.contains_key(&id) {
                    panic!("A obsoleted map file {id} still exists in a visible version");
                }
            }
        }

        if !obsoleted_page_files.is_empty() {
            info!("Clean obsoleted page files {obsoleted_page_files:?}");
            if let Err(err) = self
                .page_files
                .remove_page_files(obsoleted_page_files)
                .await
            {
                todo!("{err}");
            }
        }
        if !obsoleted_map_files.is_empty() {
            info!("Clean obsoleted map files {obsoleted_map_files:?}");
            if let Err(err) = self.page_files.remove_map_files(obsoleted_map_files).await {
                todo!("{err}");
            }
        }
    }
}
