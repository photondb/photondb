use std::{io::ErrorKind, path::PathBuf};

use prost::Message;

use super::{meta::VersionEdit, Error};
use crate::{env::*, page_store::Result};

const CURRENT_FILE_NAME: &str = "CURRENT";
const MANIFEST_FILE_NAME: &str = "MANIFEST";
const TEMPFILE_SUFFIX: &str = "tmpdb";
const MAX_MANIFEST_SIZE: u64 = 128 << 20; // 128 MiB

pub(crate) struct Manifest<E: Env> {
    env: E,
    base: PathBuf,
    base_dir: Option<E::Directory>,

    max_file_size: u64,

    current_file_num: Option<u32>,
    current_writer: Option<ManifestWriter<E::SequentialWriter>>,
}

struct ManifestWriter<W> {
    current_file_size: u64,
    current_writer: W,
}

impl<E: Env> Manifest<E> {
    // Open manifest in specified folder.
    // it will reopen manifest by find CURRENT and do some cleanup.
    pub(crate) async fn open(env: E, base: impl Into<PathBuf>) -> Result<Self> {
        let base = base.into();

        let mut manifest = Self {
            env,
            base,
            base_dir: None,
            max_file_size: MAX_MANIFEST_SIZE,
            current_file_num: Default::default(),
            current_writer: None,
        };
        manifest.create_base_dir_if_not_exist().await?;
        manifest.current_file_num = manifest.load_current().await?;
        manifest.open_base_dir().await?;
        manifest.cleanup_obsolete_files().await?;

        Ok(manifest)
    }

    async fn create_base_dir_if_not_exist(&self) -> Result<()> {
        match self.env.create_dir_all(&self.base).await {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                if !self
                    .env
                    .metadata(&self.base)
                    .await
                    .expect("open base dir fail")
                    .is_dir
                {
                    panic!("base dir is not a dir")
                }
            }
            Err(err) => panic!("open base dir fail, {}", err),
        }
        Ok(())
    }

    async fn open_base_dir(&mut self) -> Result<()> {
        let base_dir = self
            .env
            .open_dir(&self.base)
            .await
            .expect("open base dir fail");
        self.base_dir = Some(base_dir);
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
        let mut current = self.current_writer.take();
        let mut file_num = self.current_file_num.as_ref().unwrap_or(&0).to_owned();

        let rolled_path = if current.is_none()
            || current.as_ref().unwrap().current_file_size > self.max_file_size
        {
            file_num += 1;
            let path = self
                .base
                .join(format!("{}_{}", MANIFEST_FILE_NAME, file_num));
            let current_writer = self
                .env
                .open_sequential_writer(&path)
                .await
                .expect("create new manifest file fail");
            current = Some(ManifestWriter {
                current_file_size: 0,
                current_writer,
            });
            Some(path)
        } else {
            None
        };

        let mut current = current.unwrap();
        let written = if rolled_path.is_some() {
            // TODO: remove new created file when write fail.
            let base_snapshot = version_snapshot();
            let base_written = VersionEditEncoder(base_snapshot)
                .encode(&mut current.current_writer)
                .await?;
            match VersionEditEncoder(ve)
                .encode(&mut current.current_writer)
                .await
            {
                Ok(record_written) => base_written + record_written,
                Err(err) => {
                    let _ = self.env.remove_file(rolled_path.as_ref().unwrap()).await;
                    return Err(err);
                }
            }
        } else {
            VersionEditEncoder(ve)
                .encode(&mut current.current_writer)
                .await?
        } as u64;

        if rolled_path.is_some() {
            current
                .current_writer
                .sync_all()
                .await
                .expect("sync new manifest file fail");
            self.set_current(file_num).await?;
            // TODO: notify cleaner previous manifest + size, so it can be delete when need.
            self.current_file_num = Some(file_num);
        } else {
            current
                .current_writer
                .sync_data()
                .await
                .expect("sync manifest data fail");
        }

        current.current_file_size += written;

        self.current_writer = Some(current);

        Ok(())
    }

    // List current versions.
    // the caller can recovery Versions by apply each version_edits.
    pub(crate) async fn list_versions(&self) -> Result<Vec<VersionEdit>> {
        Ok(if let Some(current_file) = self.current_file_num {
            let path = self
                .base
                .join(format!("{}_{}", MANIFEST_FILE_NAME, current_file));
            let reader = self
                .env
                .open_positional_reader(path)
                .await
                .expect("open manifest fail");
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

    async fn load_current(&self) -> Result<Option<u32 /* file_num */>> {
        let curr_file_reader = match self
            .env
            .open_positional_reader(self.base.join(CURRENT_FILE_NAME))
            .await
        {
            Ok(f) => f,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => panic!("read current meet error"),
        };
        let mut file_num_bytes = vec![0u8; core::mem::size_of::<u32>()];
        curr_file_reader
            .read_exact_at(&mut file_num_bytes, 0)
            .await
            .expect("read current file fail");
        let file_num = u32::from_le_bytes(
            file_num_bytes[0..core::mem::size_of::<u32>()]
                .try_into()
                .map_err(|_| Error::Corrupted)?,
        );
        Ok(Some(file_num))
    }

    async fn set_current(&self, file_num: u32) -> Result<()> {
        {
            let tmp_path = self
                .base
                .join(format!("curr.{}.{}", file_num, TEMPFILE_SUFFIX));

            {
                let mut tmp_file = self
                    .env
                    .open_sequential_writer(&tmp_path)
                    .await
                    .expect("create tmp current fail");
                tmp_file
                    .write_all(&file_num.to_le_bytes())
                    .await
                    .expect("write file_num to tmp fail");
                tmp_file
                    .sync_all()
                    .await
                    .expect("sync tmp current file fail");
            }

            match self
                .env
                .rename(&tmp_path, self.base.join(CURRENT_FILE_NAME))
                .await
            {
                Ok(_) => Ok(()),
                Err(_err) => {
                    let _ = self.env.remove_file(&tmp_path).await;
                    // TODO: throw right error.
                    Err(Error::Corrupted)
                }
            }?;
        }
        self.base_dir
            .as_ref()
            .unwrap()
            .sync_all()
            .await
            .expect("sync base folder fail");
        Ok(())
    }

    async fn cleanup_obsolete_files(&self) -> Result<()> {
        fn is_obsolete_manifest(file_name: &str, curr_file_num: Option<u32>) -> bool {
            let file_num_str = file_name.trim_start_matches(&format!("{}_", MANIFEST_FILE_NAME));
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
        for path in self.env.read_dir(&self.base).expect("open base dir fail") {
            let file_path = path.unwrap().path();
            if let Some(ext) = file_path.extension() {
                if ext.to_str().unwrap() == TEMPFILE_SUFFIX {
                    wait_remove_paths.push(file_path.to_owned());
                    continue;
                }
            }
            if is_obsolete_manifest(
                file_path.file_name().unwrap().to_str().unwrap(),
                self.current_file_num,
            ) {
                wait_remove_paths.push(file_path.to_owned());
            }
        }

        let need_remove = !wait_remove_paths.is_empty();
        for path in wait_remove_paths {
            self.env
                .remove_file(path)
                .await
                .expect("remove obsolote file fail")
        }

        if need_remove {
            self.base_dir
                .as_ref()
                .unwrap()
                .sync_all()
                .await
                .expect("sync base dir fail");
        }

        Ok(())
    }
}

struct VersionEditEncoder(VersionEdit);

impl VersionEditEncoder {
    async fn encode<W: SequentialWriter>(&self, w: &mut W) -> Result<usize> {
        let bytes = self.0.encode_to_vec();
        w.write_all(&bytes.len().to_le_bytes())
            .await
            .expect("write version edit fail");
        w.write_all(&bytes).await.expect("write version edit fail");
        Ok(bytes.len() + core::mem::size_of::<u64>())
    }
}

struct VersionEditDecoder<R: PositionalReader> {
    reader: R,
    offset: u64,
}

impl<R: PositionalReader> VersionEditDecoder<R> {
    fn new(reader: R) -> Self {
        Self { reader, offset: 0 }
    }
    async fn next_record(&mut self) -> Result<Option<VersionEdit>> {
        let mut offset = self.offset;
        let len = {
            let mut len_bytes = vec![0u8; core::mem::size_of::<u64>()];
            match self
                .reader
                .read_exact_at(&mut len_bytes, offset as u64)
                .await
            {
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
                e @ Err(_) => e.expect("read mainfiest file fail"),
            };
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
    use crate::page_store::NewFile;

    #[inline]
    fn new_files(ids: Vec<u32>) -> Vec<NewFile> {
        ids.into_iter().map(Into::into).collect()
    }

    #[photonio::test]
    fn test_cleanup_when_restart() {
        let env = crate::env::Photon;

        let base = std::env::temp_dir().join("curr_test_restart");
        if base.try_exists().unwrap_or(false) {
            env.remove_dir_all(base.to_owned()).await.unwrap();
        }

        fn version_snapshot() -> VersionEdit {
            VersionEdit {
                new_files: vec![],
                deleted_files: vec![],
            }
        }
        {
            let mut manifest = Manifest::open(env.to_owned(), base.to_owned())
                .await
                .unwrap();
            manifest.max_file_size = 1;

            manifest
                .record_version_edit(
                    VersionEdit {
                        new_files: new_files(vec![2, 3]),
                        deleted_files: vec![1],
                    },
                    version_snapshot,
                )
                .await
                .unwrap();

            manifest
                .record_version_edit(
                    VersionEdit {
                        new_files: new_files(vec![2, 3]),
                        deleted_files: vec![1],
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        new_files: new_files(vec![2, 3]),
                        deleted_files: vec![1],
                    },
                    version_snapshot,
                )
                .await
                .unwrap();

            let tmp_path = base.join(format!("curr.{}.{}", 999, TEMPFILE_SUFFIX));

            let _ = env.open_sequential_writer(&tmp_path).await.unwrap();
            let files = env
                .read_dir(&base)
                .expect("open base dir fail")
                .into_iter()
                .count();
            assert_eq!(files, 5); // 3 data + 1 current + 1 tmp
        }
        {
            let _ = Manifest::open(env.clone(), base.to_owned()).await.unwrap();

            let files = env
                .read_dir(&base)
                .expect("open base dir fail")
                .into_iter()
                .count();
            assert_eq!(files, 2);
        }
    }

    #[photonio::test]
    fn test_roll_manifest() {
        let env = crate::env::Photon;

        let base = std::env::temp_dir().join("curr_test_roll");
        if base.try_exists().unwrap_or(false) {
            env.remove_dir_all(base.to_owned()).await.unwrap();
        }

        let ver = std::sync::Arc::new(std::sync::Mutex::new(VersionEdit {
            new_files: vec![],
            deleted_files: vec![],
        }));

        let ve_snapshot = || {
            let ver = ver.lock().unwrap();
            ver.to_owned()
        };

        let mock_apply = |ve: &VersionEdit| {
            let mut ver = ver.lock().unwrap();
            ver.new_files.extend_from_slice(&ve.new_files);
            ver.new_files
                .retain(|f| !ve.deleted_files.iter().any(|d| *d == f.id))
        };

        {
            let mut manifest = Manifest::open(env.to_owned(), base.to_owned())
                .await
                .unwrap();
            manifest.max_file_size = 100; // set a small threshold value to trigger roll
            assert_eq!(manifest.current_file_num, None);

            let ve = VersionEdit {
                new_files: new_files(vec![0]),
                deleted_files: vec![],
            };
            manifest
                .record_version_edit(ve.to_owned(), ve_snapshot)
                .await
                .unwrap();
            mock_apply(&ve);
            assert_eq!(manifest.current_file_num, Some(1));

            for i in 1..43u32 {
                let r = i.saturating_sub(10u32);
                let ve = VersionEdit {
                    new_files: new_files(vec![i]),
                    deleted_files: vec![r],
                };
                manifest
                    .record_version_edit(ve.to_owned(), ve_snapshot)
                    .await
                    .unwrap();
                mock_apply(&ve);
            }
            assert_eq!(manifest.current_file_num, Some(35));
        }

        {
            let mut manifest2 = Manifest::open(env, base.to_owned()).await.unwrap();
            let versions = manifest2.list_versions().await.unwrap();
            assert_eq!(manifest2.current_file_num, Some(35));

            let mut recover_ver = VersionEdit::default();
            for ve in versions {
                recover_ver.new_files.extend_from_slice(&ve.new_files);
                recover_ver
                    .new_files
                    .retain(|f| !ve.deleted_files.iter().any(|d| *d == f.id));
            }

            assert_eq!(recover_ver.new_files, {
                let ver = ver.lock().unwrap();
                ver.to_owned().new_files
            });

            let ve = VersionEdit {
                new_files: new_files(vec![1]),
                deleted_files: vec![],
            };
            manifest2
                .record_version_edit(ve.to_owned(), ve_snapshot)
                .await
                .unwrap(); // first write after reopen trigger roll.
            assert_eq!(manifest2.current_file_num, Some(36));
        }
    }

    #[photonio::test]
    fn test_mantain_current() {
        fn version_snapshot() -> VersionEdit {
            VersionEdit {
                new_files: vec![],
                deleted_files: vec![],
            }
        }

        let env = crate::env::Photon;

        let base = std::env::temp_dir().join("curr_test2");
        if base.try_exists().unwrap_or(false) {
            env.remove_dir_all(base.to_owned()).await.unwrap();
        }

        {
            let mut manifest = Manifest::open(env.to_owned(), base.to_owned())
                .await
                .unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        new_files: new_files(vec![2, 3]),
                        deleted_files: vec![1],
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        new_files: new_files(vec![4]),
                        deleted_files: vec![],
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        new_files: new_files(vec![5]),
                        deleted_files: vec![],
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
        }

        {
            let manifest2 = Manifest::open(env, base.to_owned()).await.unwrap();
            let versions = manifest2.list_versions().await.unwrap();
            assert_eq!(versions.len(), 4);
        }
    }
}
