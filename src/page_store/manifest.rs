use std::path::PathBuf;

use photonio::{
    fs::{File, OpenOptions},
    io::{ReadAt, Write},
};
use prost::Message;

use super::meta::VersionEdit;
use crate::page_store::Result;

const CURRENT_FILE_NAME: &str = "CURRENT";
const MANIFEST_FILE_NAME: &str = "MANIFEST";
const TEMPFILE_SUFFIX: &str = ".tmpdb";
const MAX_MANIFEST_SIZE: usize = 128 << 20; // 128 MiB

pub(crate) struct Manifest {
    base: PathBuf,

    max_file_size: usize,
    current_file_size: usize,

    current_file_num: Option<u32>,
}

impl Manifest {
    // Open manifest in specified folder.
    // it will reopen manifest by find CURRENT and do some cleanup.
    pub(crate) async fn open(base: impl Into<PathBuf>) -> Result<Self> {
        let mut manifest = Self {
            base: base.into(),
            max_file_size: MAX_MANIFEST_SIZE,
            current_file_size: Default::default(),
            current_file_num: None,
        };

        manifest.current_file_num = manifest.load_current().await?;
        manifest.current_file_size = todo!();

        manifest.cleanup_obsolete_files().await?;

        Ok(manifest)
    }

    // Record a new version_edit to manifest file.
    // it will rolling file when the file size over `max_file_size`.
    // so it need pass-in a `version_snapshot` to get current snapshot when it
    // rolling.
    pub(crate) async fn record_version_edit(
        &mut self,
        ve: VersionEdit,
        version_snapshot: impl FnOnce() -> VersionEdit,
    ) -> Result<()> {
        let (rolled, current_file_num) =
            if self.current_file_num.is_none() || self.current_file_size > self.max_file_size {
                (
                    true,
                    if let Some(current) = self.current_file_num {
                        current + 1
                    } else {
                        0
                    },
                )
            } else {
                (false, self.current_file_num.as_ref().unwrap().to_owned())
            };

        let mut writer = {
            let path = self
                .base
                .join(format!("{}_{}", MANIFEST_FILE_NAME, current_file_num));

            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(path)
                .await
                .expect("create new manifest file fail")
        };

        let written = if rolled {
            // TODO: remove new created file when write fail.
            let base_snapshot = version_snapshot();
            let base_written = VersionEditEncoder(base_snapshot)
                .encode(&mut writer)
                .await?;
            let record_written = VersionEditEncoder(ve).encode(&mut writer).await?;
            base_written + record_written
        } else {
            VersionEditEncoder(ve).encode(&mut writer).await?
        };

        if rolled {
            // TODO: set_current sync_all base dir
            self.set_current(current_file_num).await?;
            // TODO: notify cleaner previous manifest + size, so it can be delete when need.
            self.current_file_num = Some(current_file_num);
        } else {
            writer.sync_data().await.expect("sync manifest data fail");
        }

        self.current_file_size = if rolled {
            written
        } else {
            self.current_file_size + written
        };

        Ok(())
    }

    // List current versions.
    // the caller can recovery Versions by apply each version_edits.
    pub(crate) async fn list_versions(&self) -> Result<Vec<VersionEdit>> {
        Ok(if let Some(current_file) = self.current_file_num {
            let path = self
                .base
                .join(format!("{}_{}", MANIFEST_FILE_NAME, current_file));
            let reader = File::open(path).await.expect("manifest not found");
            let mut decoder = VersionEditDecoder::new(reader);
            let mut ves = Vec::new();
            while let Some(ve) = decoder.next_record().await.expect("manifest decode error") {
                ves.push(ve)
            }
            ves
        } else {
            vec![]
        })
    }

    async fn load_current(&self) -> Result<Option<u32>> /* file_num */ {
        todo!("load file_num in current if exist")
    }

    async fn set_current(&self, file_num: u32) -> Result<()> {
        todo!("write tmp + rename and sync_all")
    }

    async fn cleanup_obsolete_files(&self) -> Result<()> {
        todo!("1. remove TEMPFILE_SUFFIX files; 2. remove MANIFEST number < CURRENT")
    }
}

struct VersionEditEncoder(VersionEdit);

impl VersionEditEncoder {
    async fn encode(&self, w: &mut File) -> Result<usize> {
        todo!("encode record: LEN + self.0.encode and return writen bytes")
    }
}

struct VersionEditDecoder<R: ReadAt> {
    reader: R,
    offset: usize,
}

impl<R: ReadAt> VersionEditDecoder<R> {
    fn new(reader: R) -> Self {
        Self { reader, offset: 0 }
    }
    async fn next_record(&mut self) -> Result<Option<VersionEdit>> {
        todo!("decode one record")
    }
}
