use crossbeam_epoch::Guard;

use super::Result;

struct BTree {}

impl BTree {
    async fn get<'g>(&self, key: &[u8], guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        todo!()
    }

    async fn put<'g>(&self, key: &[u8], value: &[u8], guard: &'g Guard) -> Result<()> {
        todo!()
    }

    async fn delete<'g>(&self, key: &[u8], guard: &'g Guard) -> Result<()> {
        todo!()
    }
}
