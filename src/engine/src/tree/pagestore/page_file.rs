use crate::env::PositionalRead;

struct Handle {
    pub offset: u64,
    pub size: u64,
}

struct Footer {
    pub meta_handle: Handle,
    pub index_handle: Handle,
    pub magic_number: u64,
}

pub struct PageFile<R> {
    file: R,
}

impl<R: PositionalRead> PageFile<R> {
    pub fn offset(&self) -> u64 {
        todo!()
    }

    pub fn num_pages(&self) -> usize {
        todo!()
    }

    pub fn page_head(&self, offset: u64) {
        todo!()
    }

    pub async fn read_page(&self, offset: u64) {
        todo!()
    }
}
