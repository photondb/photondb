use std::sync::Arc;

use super::page_file::PageFile;
use crate::env::Env;

pub struct Manifest<E> {
    files: Vec<Arc<PageFile<E>>>,
}

impl<E: Env> Manifest<E> {
    fn find_file(&self, offset: u64) -> Option<&PageFile<E::PositionalFile>> {
        todo!()
    }
}

pub struct ManifestList<E> {
    manifests: Vec<Manifest<E>>,
}

impl<E: Env> ManifestList<E> {
    fn current(&self) -> Manifest<E> {
        todo!()
    }
}

pub struct ManifestEdit {}
