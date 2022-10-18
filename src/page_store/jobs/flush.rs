use crate::page_store::Version;

pub(crate) struct FlushCtx {
    // TODO: cancel task
}

impl FlushCtx {
    pub async fn run(self, _version: Version) {
        loop {
            todo!("wait flushable write buffers and flush them to disk")
        }
    }
}
