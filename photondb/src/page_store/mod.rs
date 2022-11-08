use std::{collections::HashSet, fmt, mem, path::Path, sync::Arc};

use crate::{env::Env, util::shutdown::ShutdownNotifier};

mod error;
pub(crate) use error::{Error, Result};

mod page_txn;
use futures::lock::Mutex;
pub(crate) use page_txn::Guard;

mod page_table;
use page_table::PageTable;
pub(crate) use page_table::{MIN_ID, NAN_ID};

mod meta;
pub(crate) use meta::{NewFile, VersionEdit};

mod version;
use version::{Version, VersionOwner};

mod jobs;
pub(crate) use jobs::RewritePage;
use jobs::{cleanup::CleanupCtx, flush::FlushCtx, gc::GcCtx};

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
#[derive(Clone, Debug)]
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

pub(crate) struct PageStore<E: Env> {
    #[allow(unused)]
    options: Options,
    #[allow(unused)]
    env: E,
    table: PageTable,

    version_owner: Arc<VersionOwner>,
    page_files: Arc<PageFiles<E>>,
    #[allow(unused)]
    manifest: Arc<Mutex<Manifest<E>>>,

    jobs: Vec<E::JoinHandle<()>>,
    shutdown: ShutdownNotifier,
}

impl<E: Env> PageStore<E> {
    pub(crate) async fn open<P, R>(env: E, path: P, options: Options, rewriter: R) -> Result<Self>
    where
        P: AsRef<Path>,
        R: RewritePage<E>,
    {
        let (next_file_id, manifest, table, page_files, file_infos) =
            Self::recover(env.to_owned(), path).await?;

        let version = Version::new(
            options.write_buffer_capacity,
            next_file_id,
            file_infos,
            HashSet::default(),
        );

        let version_owner = Arc::new(VersionOwner::new(version));
        let manifest = Arc::new(futures::lock::Mutex::new(manifest));
        let page_files = Arc::new(page_files);
        let shutdown = ShutdownNotifier::new();

        let mut store = PageStore {
            options,
            env,
            table,
            version_owner,
            page_files,
            manifest,
            jobs: Vec::new(),
            shutdown,
        };

        // Spawn background jobs.
        store.spawn_flush_job();
        store.spawn_cleanup_job();
        store.spawn_gc_job(rewriter);

        Ok(store)
    }

    #[inline]
    pub(crate) fn guard(&self) -> Guard<E> {
        Guard::new(self.version(), &self.table, &self.page_files)
    }

    pub(crate) async fn close(mut self) {
        self.shutdown.terminate();
        let jobs = mem::take(&mut self.jobs);
        for job in jobs {
            job.await;
        }
    }

    #[inline]
    fn version(&self) -> Arc<Version> {
        self.version_owner.current()
    }

    fn spawn_flush_job(&mut self) {
        let job = FlushCtx::new(
            self.shutdown.subscribe(),
            self.version_owner.clone(),
            self.page_files.clone(),
            self.manifest.clone(),
        );
        let handle = self.env.spawn_background(job.run());
        self.jobs.push(handle);
    }

    fn spawn_cleanup_job(&mut self) {
        let job = CleanupCtx::new(self.shutdown.subscribe(), self.page_files.clone());
        let handle = self.env.spawn_background(job.run(self.version()));
        self.jobs.push(handle);
    }

    fn spawn_gc_job<R>(&mut self, rewriter: R)
    where
        R: RewritePage<E>,
    {
        let strategy_builder = Box::new(MinDeclineRateStrategyBuilder::new(1 << 30, usize::MAX));
        let job = GcCtx::new(
            self.shutdown.subscribe(),
            rewriter,
            strategy_builder,
            self.table.clone(),
            self.page_files.clone(),
        );
        let handle = self.env.spawn_background(job.run(self.version()));
        self.jobs.push(handle);
    }
}

impl<E: Env> Drop for PageStore<E> {
    fn drop(&mut self) {
        self.shutdown.terminate();
    }
}

impl<E: Env> fmt::Debug for PageStore<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageStore").finish()
    }
}
