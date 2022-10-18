use std::collections::HashSet;

use crate::page_store::{FileInfo, Version};

pub(crate) struct RecycleCtx {
    // TODO: cancel task
}

impl RecycleCtx {
    pub async fn run(self, mut version: Version) {
        loop {
            self.do_recycle(&version).await;
            version = version.wait_next_version().await;
        }
    }

    async fn do_recycle(&self, version: &Version) {
        let mut recycle_file_set = HashSet::new();
        for (_, file) in &version.files {
            if !self.should_recycle_file(file) {
                continue;
            }
            recycle_file_set.insert(file.get_file_id());
        }

        todo!("find the most appropriated file")
    }

    fn should_recycle_file(&self, file: &FileInfo) -> bool {
        todo!()
    }

    async fn forward_active_pages(&self, file: &FileInfo) {
        todo!()
    }
}
