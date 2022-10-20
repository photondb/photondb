use std::{io::ErrorKind, path::PathBuf};

use photonio::{
    fs::{File, OpenOptions},
    io::{ReadAt, ReadAtExt, ReadExt, WriteExt},
};
use prost::Message;

use super::{meta::VersionEdit, Error};
use crate::page_store::Result;

const CURRENT_FILE_NAME: &str = "CURRENT";
const MANIFEST_FILE_NAME: &str = "MANIFEST";
const TEMPFILE_SUFFIX: &str = ".tmpdb";
const MAX_MANIFEST_SIZE: u64 = 128 << 20; // 128 MiB

pub(crate) struct Manifest {
    base: PathBuf,

    max_file_size: u64,
    current_file_size: u64,

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
        manifest.create_base_dir_if_not_exist()?;
        manifest.current_file_num = manifest.load_current().await?;
        manifest.current_file_size = manifest.file_size().await?;
        manifest.cleanup_obsolete_files().await?;
        Ok(manifest)
    }

    fn create_base_dir_if_not_exist(&self) -> Result<()> {
        match std::fs::create_dir_all(&self.base) {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                if !std::fs::metadata(&self.base)
                    .expect("open base dir fail")
                    .is_dir()
                {
                    panic!("base dir is not a dir")
                }
            }
            Err(err) => panic!("open base dir fail, {}", err),
        }
        Ok(())
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
        } as u64;

        if rolled {
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
            let file_size = reader
                .metadata()
                .await
                .expect("read metadata of manifest fail")
                .len();
            let mut decoder = VersionEditDecoder::new(reader, file_size);
            let mut ves = Vec::new();
            while let Some(ve) = decoder.next_record().await.expect("manifest decode error") {
                ves.push(ve)
            }
            ves
        } else {
            vec![]
        })
    }

    async fn load_current(&self) -> Result<Option<u32 /* file_num */>> {
        let mut curr_file = match File::open(self.base.join(CURRENT_FILE_NAME)).await {
            Ok(f) => f,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => panic!("read current meet error"),
        };
        let mut file_num_bytes = vec![0u8; core::mem::size_of::<u32>()];
        curr_file
            .read_exact(&mut file_num_bytes)
            .await
            .expect("read current file fail");
        let file_num = u32::from_le_bytes(
            file_num_bytes[0..core::mem::size_of::<u32>()]
                .try_into()
                .map_err(|_| Error::Corrupted)?,
        );
        Ok(Some(file_num))
    }

    async fn file_size(&self) -> Result<u64> {
        Ok(if let Some(curr) = self.current_file_num {
            let manifest_file =
                File::open(self.base.join(format!("{}_{}", MANIFEST_FILE_NAME, curr)))
                    .await
                    .expect("read manifest fail");
            manifest_file
                .metadata()
                .await
                .expect("read manifest fail")
                .len()
        } else {
            0
        })
    }

    async fn set_current(&self, file_num: u32) -> Result<()> {
        {
            let tmp_path = self
                .base
                .join(format!("curr.{}{}", file_num, TEMPFILE_SUFFIX));

            let mut tmp_file = File::create(&tmp_path)
                .await
                .expect("create tmp current fail");
            tmp_file
                .write_all(&file_num.to_le_bytes())
                .await
                .expect("write file_num to tmp fail");

            match photonio::fs::rename(&tmp_path, self.base.join(CURRENT_FILE_NAME)).await {
                Ok(_) => Ok(()),
                Err(_err) => {
                    let _ = photonio::fs::remove_file(&tmp_path).await;
                    // TODO: throw right error.
                    Err(Error::Corrupted)
                }
            }?;
        }
        {
            let base_dir = File::open(&self.base).await.expect("open base folder fail");
            base_dir.sync_all().await.expect("sync base folder fail");
        }
        Ok(())
    }

    async fn cleanup_obsolete_files(&self) -> Result<()> {
        fn is_obsolete_manifest(file_name: &str, curr_file_num: Option<u32>) -> bool {
            let file_num_str = file_name.trim_end_matches(&format!("{}_", MANIFEST_FILE_NAME));
            if let Ok(file_num) = file_num_str.parse::<u32>() {
                if let Some(curr_file_num) = curr_file_num {
                    if file_num < curr_file_num {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        }

        let mut wait_remove_paths = Vec::new();
        for path in std::fs::read_dir(&self.base).expect("open base dir fail") {
            let file_path = path.unwrap().path();
            if file_path.ends_with(TEMPFILE_SUFFIX) {
                wait_remove_paths.push(file_path.to_owned());
                continue;
            }
            if is_obsolete_manifest(
                file_path.file_name().unwrap().to_str().unwrap(),
                self.current_file_num,
            ) {
                wait_remove_paths.push(file_path.to_owned());
            }
        }

        for path in wait_remove_paths {
            photonio::fs::remove_file(path)
                .await
                .expect("remove obsolote file fail")
        }

        Ok(())
    }
}

struct VersionEditEncoder(VersionEdit);

impl VersionEditEncoder {
    async fn encode(&self, w: &mut File) -> Result<usize> {
        let bytes = self.0.encode_to_vec();
        w.write_all(&bytes.len().to_le_bytes())
            .await
            .expect("write version edit fail");
        w.write_all(&bytes).await.expect("write version edit fail");
        Ok(bytes.len() + core::mem::size_of::<u64>())
    }
}

struct VersionEditDecoder<R: ReadAt> {
    reader: R,
    offset: u64,
    file_size: u64,
}

impl<R: ReadAt> VersionEditDecoder<R> {
    fn new(reader: R, file_size: u64) -> Self {
        Self {
            reader,
            offset: 0,
            file_size,
        }
    }
    async fn next_record(&mut self) -> Result<Option<VersionEdit>> {
        let mut offset = self.offset;
        if offset == self.file_size {
            return Ok(None);
        }
        let len = {
            let mut len_bytes = vec![0u8; core::mem::size_of::<u64>()];
            self.reader
                .read_exact_at(&mut len_bytes, offset as u64)
                .await
                .expect("read version edit record len fail");
            u64::from_le_bytes(
                len_bytes[0..core::mem::size_of::<u64>()]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            )
        };
        offset += core::mem::size_of::<u64>() as u64;
        let ve = {
            let mut ve_bytes = vec![0u8; len as usize];
            self.reader
                .read_exact_at(&mut ve_bytes, offset as u64)
                .await
                .expect("read version edit record payload fail");
            VersionEdit::decode(ve_bytes.as_slice()).expect("decode version edit fail")
        };
        self.offset = offset + len;
        Ok(Some(ve))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[photonio::test]
    fn test_mantain_current() {
        fn version_snapshot() -> VersionEdit {
            VersionEdit {
                new_files: vec![],
                deleted_files: vec![],
            }
        }

        let base = std::env::temp_dir().join("curr_test2");
        std::fs::remove_dir_all(base.to_owned()).unwrap();

        {
            let mut manifest = Manifest::open(base.to_owned()).await.unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        new_files: vec![2, 3],
                        deleted_files: vec![1],
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        new_files: vec![4],
                        deleted_files: vec![],
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        new_files: vec![5],
                        deleted_files: vec![],
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
        }

        {
            let manifest2 = Manifest::open(base.to_owned()).await.unwrap();
            let versions = manifest2.list_versions().await.unwrap();
            assert_eq!(versions.len(), 4);
        }
    }
}
