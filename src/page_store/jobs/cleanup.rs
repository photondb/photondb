use std::rc::Rc;

use crate::page_store::Version;

pub(crate) struct CleanupCtx {
    // TODO: cancel task
}

impl CleanupCtx {
    pub async fn run(self, mut version: Version) {
        loop {
            let deleted_files = version.deleted_files();

            let mut next_version = version.wait_next_version().await;
            std::mem::swap(&mut next_version, &mut version);
            wait_version_released(next_version).await;

            // Now it is safety to cleanup the version.
            todo!()
        }
    }
}

async fn wait_version_released(version: Version) {
    version.wait_version_released().await;

    todo!()
}
