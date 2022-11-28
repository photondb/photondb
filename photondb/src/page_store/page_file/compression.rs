use bitflags::bitflags;

use crate::page_store::{Error, Result};

bitflags! {
    pub struct Compression: u8 {
        const NONE = 1;
        const SNAPPY = 2;
        const ZSTD = 4;
    }
}

pub(crate) fn decompress_into(
    compression: Compression,
    input: &[u8],
    target: &mut Vec<u8>,
) -> Result<()> {
    match compression {
        Compression::NONE => Ok(()),
        Compression::SNAPPY => {
            let len = snap::raw::Decoder::new()
                .decompress(input, target)
                .map_err(|_| Error::Corrupted)?;
            target.truncate(len);
            Ok(())
        }
        Compression::ZSTD => {
            let mut decompressor = zstd::bulk::Decompressor::new().unwrap();
            let len = decompressor
                .decompress_to_buffer(input, target)
                .map_err(|_| Error::Corrupted)?;
            target.truncate(len);
            Ok(())
        }
        _ => unreachable!(),
    }
}

pub(crate) fn decompress_len(
    compression: Compression,
    input: &[u8],
) -> Result<(usize /* decompress len */, usize /* skip */)> {
    match compression {
        Compression::NONE => Ok((0, 0)),
        Compression::SNAPPY => {
            let len = snap::raw::decompress_len(input).map_err(|_| Error::Corrupted)?;
            Ok((len, 0))
        }
        Compression::ZSTD => {
            let raw_len = u64::from_le_bytes(
                input[..std::mem::size_of::<u64>()]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            );
            Ok((raw_len as usize, std::mem::size_of::<u64>()))
        }
        _ => unreachable!(),
    }
}

pub(crate) fn compress_page<'a>(
    compression: Compression,
    raw: &'a [u8],
    compress_buf: &'a mut [u8],
) -> Result<&'a [u8]> {
    match compression {
        Compression::NONE => Ok(raw),
        Compression::SNAPPY => {
            let len = snap::raw::Encoder::new()
                .compress(raw, compress_buf)
                .map_err(|_| Error::Corrupted)?;
            Ok(&compress_buf[..len])
        }
        Compression::ZSTD => {
            let raw_len = raw.len() as u64;
            compress_buf[0..std::mem::size_of::<u64>()].copy_from_slice(&raw_len.to_le_bytes());
            let dat_size = {
                let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
                compressor
                    .compress_to_buffer(raw, &mut compress_buf[std::mem::size_of::<u64>()..])
                    .unwrap()
            } + std::mem::size_of::<u64>();
            Ok(&compress_buf[..dat_size])
        }
        _ => unreachable!(),
    }
}

pub(crate) fn compress_max_len(compression: Compression, input: &[u8]) -> usize {
    match compression {
        Compression::NONE => 0,
        Compression::SNAPPY => snap::raw::max_compress_len(input.len()),
        Compression::ZSTD => zstd_safe::compress_bound(input.len()) + std::mem::size_of::<u64>(),
        _ => unreachable!(),
    }
}
