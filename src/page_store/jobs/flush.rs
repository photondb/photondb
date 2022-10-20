use std::sync::Arc;

use crate::{
    page_store::{PageFiles, RecordRef, Version, WriteBuffer},
    Result,
};

#[allow(dead_code)]
pub(crate) struct FlushCtx {
    // TODO: cancel task
    page_files: Arc<PageFiles>,
}

#[allow(dead_code)]
impl FlushCtx {
    pub(crate) async fn run(self, _version: Version) {
        loop {
            todo!("wait flushable write buffers and flush them to disk")
        }
    }

    async fn flush(&self, write_buffer: Arc<WriteBuffer>) -> Result<()> {
        assert!(write_buffer.is_flushable());
        let file_id = write_buffer.file_id();
        let mut builder = self.page_files.new_file_builder(file_id).await?;
        for (page_addr, header, record_ref) in write_buffer.iter() {
            match record_ref {
                RecordRef::DeletedPages(deleted_pages) => {
                    builder.add_delete_pages(deleted_pages.as_slice());
                }
                RecordRef::Page(page) => {
                    let content = page.as_slice();
                    builder
                        .add_page(header.page_id(), page_addr, content)
                        .await?;
                }
            }
        }
        builder.finish().await?;
        Ok(())
    }
}
