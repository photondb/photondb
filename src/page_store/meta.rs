use prost::{alloc::vec::Vec, Message};

#[allow(unreachable_pub)]
#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Message)]
pub(crate) struct NewFile {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(uint32, tag = "2")]
    pub up1: u32,
    #[prost(uint32, tag = "3")]
    pub up2: u32,
}

#[allow(unreachable_pub)]
#[derive(Clone, PartialEq, Message)]
pub(crate) struct VersionEdit {
    #[prost(message, repeated, tag = "1")]
    pub new_files: Vec<NewFile>,
    #[prost(uint32, repeated, tag = "2")]
    pub deleted_files: Vec<u32>,
}

mod convert {
    use super::*;
    use crate::page_store::FileInfo;

    impl From<&FileInfo> for NewFile {
        fn from(info: &FileInfo) -> Self {
            NewFile {
                id: info.get_file_id(),
                up1: info.up1(),
                up2: info.up2(),
            }
        }
    }

    impl From<u32> for NewFile {
        fn from(file_id: u32) -> Self {
            NewFile {
                id: file_id,
                up1: file_id,
                up2: file_id,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_edit_decode_and_encode() {
        let new_files: Vec<NewFile> = vec![4, 5, 6].into_iter().map(Into::into).collect();
        let edit = VersionEdit {
            new_files: new_files.clone(),
            deleted_files: vec![1, 2, 3],
        };

        let payload = edit.encode_to_vec();
        let edit = VersionEdit::decode(payload.as_slice()).unwrap();
        assert_eq!(edit.deleted_files, vec![1, 2, 3]);
        assert_eq!(edit.new_files, new_files,);
    }
}
