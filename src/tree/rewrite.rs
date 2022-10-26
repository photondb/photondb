use std::sync::Arc;

use super::Tree;
use crate::{env::Env, page_store::RewritePage};

pub(crate) struct PageRewriter<E: Env> {
    tree: Arc<Tree<E>>,
}

impl<E: Env> PageRewriter<E> {
    pub(crate) fn new(tree: Arc<Tree<E>>) -> Self {
        PageRewriter { tree }
    }
}

#[async_trait::async_trait]
impl<E: Env> RewritePage for PageRewriter<E> {
    async fn rewrite(&self, page_id: u64) -> crate::page_store::Result<()> {
        self.tree.rewrite(page_id).await?;
        Ok(())
    }
}
