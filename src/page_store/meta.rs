use prost::{alloc::vec::Vec, Message};

#[derive(Clone, PartialEq, Message)]
pub(crate) struct VersionEdit {
    #[prost(uint32, repeated, tag = "1")]
    pub deleted_file_set: Vec<u32>,
    #[prost(uint32, repeated, tag = "2")]
    pub new_files: Vec<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_edit_decode_and_encode() {
        let edit = VersionEdit {
            deleted_file_set: vec![1, 2, 3],
            new_files: vec![4, 5, 6],
        };

        let payload = edit.encode_to_vec();
        let edit = VersionEdit::decode(payload.as_slice()).unwrap();
        assert_eq!(edit.deleted_file_set, vec![1, 2, 3]);
        assert_eq!(edit.new_files, vec![4, 5, 6]);
    }
}
