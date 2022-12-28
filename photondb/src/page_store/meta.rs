use prost::{alloc::vec::Vec, Message};

/// A page file or map file.
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

/// A sequence of ordered files forms a stream.
#[allow(unreachable_pub)]
#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Message)]
pub(crate) struct StreamEdit {
    #[prost(message, repeated, tag = "1")]
    pub new_files: Vec<NewFile>,
    #[prost(uint32, repeated, tag = "2")]
    pub deleted_files: Vec<u32>,
}

#[allow(unreachable_pub)]
#[derive(Clone, PartialEq, Message)]
pub(crate) struct VersionEdit {
    /// A set of map files.
    #[prost(message, tag = "1")]
    pub file_stream: Option<StreamEdit>,
}

mod convert {
    use super::*;
    use crate::page_store::FileInfo;

    impl From<u32> for NewFile {
        fn from(file_id: u32) -> Self {
            NewFile {
                id: file_id,
                up1: file_id,
                up2: file_id,
            }
        }
    }

    impl From<&FileInfo> for NewFile {
        fn from(info: &FileInfo) -> Self {
            NewFile {
                id: info.meta().file_id,
                up1: info.up1(),
                up2: info.up2(),
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
            file_stream: Some(StreamEdit {
                new_files,
                deleted_files: vec![1, 2, 3],
            }),
        };

        let payload = edit.encode_to_vec();
        let new = VersionEdit::decode(payload.as_slice()).unwrap();
        assert_eq!(edit, new);
    }
}
