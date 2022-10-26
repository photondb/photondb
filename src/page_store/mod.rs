use std::{
    collections::HashSet,
    path::Path,
    rc::Rc,
    sync::{Arc, Mutex, Weak},
};

use crate::{env::Env, Options};

mod error;
pub(crate) use error::{Error, Result};

mod page_txn;
use futures::future::BoxFuture;
pub(crate) use page_txn::{Guard, PageTxn};

mod page_table;
use page_table::PageTable;
pub(crate) use page_table::{MAX_ID, MIN_ID, NAN_ID};

mod meta;
pub(crate) use meta::VersionEdit;

mod version;
use version::Version;

mod jobs;
mod write_buffer;
pub(crate) use write_buffer::{RecordRef, WriteBuffer};

mod manifest;
pub(crate) use manifest::Manifest;

mod page_file;
pub(crate) use page_file::{FileInfo, PageFiles, PageHandle};

use self::jobs::gc::{GcPickStrategy, RewritePage};

mod recover;

pub(crate) struct PageStore<E> {
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
    manifest: Arc<futures::lock::Mutex<Manifest>>,
    _job_handle: JobHandle,
}

impl<E: Env> PageStore<E> {
    pub(crate) async fn open<P: AsRef<Path>>(env: E, path: P, options: Options) -> Result<Self> {
        let (next_file_id, manifest, table, page_files, file_infos) = Self::recover(path).await?;

        let version = Version::new(
            options.write_buffer_capacity,
            next_file_id,
            file_infos,
            HashSet::default(),
        );

        let version = Arc::new(Mutex::new(version));
        let manifest = Arc::new(futures::lock::Mutex::new(manifest));
        let page_files = Arc::new(page_files);

        let job_handle = JobHandle::new(
            &env,
            page_files.clone(),
            version.clone(),
            manifest.clone(),
            todo!(),
            todo!(),
        );

        Ok(PageStore {
            options,
            env,
            table,
            version,
            page_files,
            manifest,
            _job_handle: job_handle,
        })
    }

    pub(crate) fn guard(&self) -> Guard {
        Guard::new(
            self.current_version(),
            self.table.clone(),
            self.page_files.clone(),
        )
    }

    #[inline]
    fn current_version(&self) -> Rc<Version> {
        Version::from_local().unwrap_or_else(|| {
            let version = Rc::new(self.global_version());
            Version::set_local(version);
            Version::from_local().expect("Already installed")
        })
    }

    #[inline]
    fn global_version(&self) -> Version {
        self.version.lock().expect("Poisoned").clone()
    }
}

pub(crate) struct JobHandle {
    flush_task: Option<BoxFuture<'static, ()>>,
    cleanup_task: Option<BoxFuture<'static, ()>>,
    gc_task: Option<BoxFuture<'static, ()>>,
}

impl JobHandle {
    pub(crate) fn new<E: Env>(
        env: &E,
        page_files: Arc<PageFiles>,
        version: Arc<Mutex<Version>>,
        manifest: Arc<futures::lock::Mutex<Manifest>>,
        rewrite: Weak<dyn RewritePage>,
        pick_strategy: Box<dyn GcPickStrategy>,
    ) -> JobHandle {
        use self::jobs::{cleanup::CleanupCtx, flush::FlushCtx, gc::GcCtx};

        let cleanup_ctx = CleanupCtx::new(page_files.clone());
        let global_version = { version.lock().expect("Poisoned").clone() };
        let cloned_global_version = global_version.clone();
        let cleanup_task = env.spawn_background(async {
            cleanup_ctx.run(cloned_global_version).await;
        });

        let flush_ctx = FlushCtx::new(version.clone(), page_files.clone(), manifest.clone());
        let flush_task = env.spawn_background(async {
            flush_ctx.run().await;
        });

        let gc_ctx = GcCtx::new(rewrite, pick_strategy, page_files);
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
