use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::env::Env;

mod error;
pub(crate) use error::{Error, Result};

mod page_txn;
use futures::future::BoxFuture;
pub(crate) use page_txn::Guard;

mod page_table;
use page_table::PageTable;
pub(crate) use page_table::{MIN_ID, NAN_ID};

mod meta;
pub(crate) use meta::{NewFile, VersionEdit};

mod version;
use version::Version;

mod jobs;
pub(crate) use jobs::RewritePage;

mod write_buffer;
pub(crate) use write_buffer::{RecordRef, WriteBuffer};

mod manifest;
pub(crate) use manifest::Manifest;

mod page_file;
pub(crate) use page_file::{FileInfo, PageFiles};

mod recover;
mod strategy;
pub(crate) use strategy::{MinDeclineRateStrategyBuilder, StrategyBuilder};

/// Options to configure a page store.
#[non_exhaustive]
#[derive(Clone)]
pub struct Options {
    /// The capacity of [`WriteBuffer`]. It should be power of two.
    ///
    /// Default: 128MB
    pub write_buffer_capacity: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            write_buffer_capacity: 128 << 20,
        }
    }
}

pub(crate) struct PageStore<E: Env>
where
    Self: Send + Sync,
{
    #[allow(unused)]
    options: Options,
    #[allow(unused)]
    env: E,
    table: PageTable,

    /// The global [`Version`] of [`PageStore`], used when tls [`Version`] does
    /// not exist. It needs to be updated every time a new [`Version`] is
    /// installed.
    version: Arc<Mutex<Version>>,

    page_files: Arc<PageFiles>,
    #[allow(unused)]
    manifest: Arc<futures::lock::Mutex<Manifest<E>>>,
}

impl<E: Env> PageStore<E> {
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let (next_file_id, manifest, table, page_files, file_infos) =
            Self::recover(env.to_owned(), path).await?;

        let version = Version::new(
            options.write_buffer_capacity,
            next_file_id,
            file_infos,
            HashSet::default(),
        );

        let version = Arc::new(Mutex::new(version));
        let manifest = Arc::new(futures::lock::Mutex::new(manifest));
        let page_files = Arc::new(page_files);

        Ok(PageStore {
            options,
            env,
            table,
            version,
            page_files,
            manifest,
        })
    }

    pub(crate) fn guard(&self) -> Guard {
        Guard::new(self.current_version(), &self.table, &self.page_files)
    }

    #[inline]
    fn current_version(&self) -> Arc<Version> {
        Version::from_local().unwrap_or_else(|| {
            let version = Arc::new(self.global_version());
            Version::set_local(version);
            Version::from_local().expect("Already installed")
        })
    }

    #[inline]
    fn global_version(&self) -> Version {
        self.version.lock().expect("Poisoned").clone()
    }

    #[inline]
    fn env(&self) -> &E {
        &self.env
    }
}

pub(crate) struct JobHandle {
    flush_task: Option<BoxFuture<'static, ()>>,
    cleanup_task: Option<BoxFuture<'static, ()>>,
    gc_task: Option<BoxFuture<'static, ()>>,
}

impl JobHandle {
    pub(crate) fn new<E: Env + 'static>(
        page_store: &PageStore<E>,
        rewriter: Box<dyn RewritePage>,
        strategy_builder: Box<dyn StrategyBuilder>,
    ) -> JobHandle {
        use self::jobs::{cleanup::CleanupCtx, flush::FlushCtx, gc::GcCtx};

        let env = page_store.env();
        let page_table = page_store.table.clone();
        let page_files = page_store.page_files.clone();
        let version = page_store.version.clone();
        let manifest = page_store.manifest.clone();

        let cleanup_ctx = CleanupCtx::new(page_files.clone());
        let global_version = { version.lock().expect("Poisoned").clone() };
        let cloned_global_version = global_version.clone();
        let cleanup_task = env.spawn_background(async {
            cleanup_ctx.run(cloned_global_version).await;
        });

        let flush_ctx = FlushCtx::new(version, page_files.clone(), manifest);
        let flush_task = env.spawn_background(async {
            flush_ctx.run().await;
        });

        let gc_ctx = GcCtx::new(rewriter, strategy_builder, page_table, page_files);
        let gc_task = env.spawn_background(async {
            gc_ctx.run(global_version).await;
        });

        JobHandle {
            flush_task: Some(flush_task),
            cleanup_task: Some(cleanup_task),
            gc_task: Some(gc_task),
        }
    }
}

impl Drop for JobHandle {
    fn drop(&mut self) {
        futures::executor::block_on(async {
            if let Some(task) = self.flush_task.take() {
                task.await;
            }
            if let Some(task) = self.cleanup_task.take() {
                task.await;
            }
            if let Some(task) = self.gc_task.take() {
                task.await;
            }
        });
    }
}
