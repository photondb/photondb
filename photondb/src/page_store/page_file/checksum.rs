use bitflags::bitflags;

use crate::page_store::{Error, Result};

bitflags! {
/// Checksum Type.
pub struct ChecksumType: u8 {
    /// Without checksum.
    const NONE = 0;
    /// Checksum with CRC32.
    const CRC32 = 1;
    /// Checksum with XXHASH.
    #[allow(dead_code)]
    const XXHASH = 2;
}
}

pub(crate) fn checksum(typ: ChecksumType, content: &[u8]) -> Option<u32> {
    match typ {
        ChecksumType::NONE => None,
        ChecksumType::CRC32 => Some(crc32fast::hash(content)),
        ChecksumType::XXHASH => todo!(),
        _ => unreachable!(),
    }
}

pub(crate) fn check_checksum(typ: ChecksumType, content: &[u8], checksum_val: u32) -> Result<()> {
    if let Some(file_checksum) = checksum(typ, content) {
        if file_checksum != checksum_val {
            return Err(Error::Corrupted);
        }
    }
    Ok(())
}
