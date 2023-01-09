use super::file_builder::*;
use crate::{
    env::{PositionalReader, PositionalReaderExt},
    page_store::Result,
    util::atomic::Counter,
};

pub(crate) struct FileReader<R: PositionalReader> {
    reader: R,
    use_direct: bool,
    pub(super) align_size: usize,
    pub(super) file_size: usize,
    read_bytes: Counter,
}

impl<R: PositionalReader> FileReader<R> {
    /// Open page reader.
    pub(super) fn from(reader: R, use_direct: bool, align_size: usize, file_size: usize) -> Self {
        Self {
            reader,
            use_direct,
            align_size,
            file_size,
            read_bytes: Counter::new(0),
        }
    }

    /// Reads the exact number of bytes from the page specified by `offset`.
    pub(crate) async fn read_exact_at(&self, buf: &mut [u8], req_offset: u64) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        if !self.use_direct {
            self.reader.read_exact_at(buf, req_offset).await?;
            self.read_bytes.add(buf.len() as u64);
            return Ok(());
        }

        let align_offset = floor_to_block_lo_pos(req_offset as usize, self.align_size);
        let offset_ahead = (req_offset as usize) - align_offset;
        let align_buf_size =
            ceil_to_block_hi_pos(req_offset as usize + buf.len(), self.align_size) - align_offset;

        let mut align_buf = AlignBuffer::new(align_buf_size, self.align_size); // TODO: pool this buf?
        let read_buf = align_buf.as_bytes_mut();

        self.inner_read_exact_at(&self.reader, read_buf, align_offset as u64)
            .await?;

        buf.copy_from_slice(&read_buf[offset_ahead..offset_ahead + buf.len()]);
        self.read_bytes.add(buf.len() as u64);

        Ok(())
    }

    async fn inner_read_exact_at(
        &self,
        r: &R,
        mut buf: &mut [u8],
        mut pos: u64,
    ) -> std::io::Result<()> {
        assert!(is_block_aligned_ptr(buf.as_ptr(), self.align_size));
        assert!(is_block_aligned_pos(pos as usize, self.align_size));
        while !buf.is_empty() {
            match r.read_at(buf, pos).await {
                Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    buf = &mut buf[n..];
                    pos += n as u64;
                    if !is_block_aligned_pos(n, self.align_size) {
                        // only happen when end of file.
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub(crate) async fn read_block(&self, block_handle: BlockHandle) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; block_handle.length as usize];
        self.read_exact_at(&mut buf, block_handle.offset).await?;
        Ok(buf)
    }

    #[inline]
    pub(crate) fn total_read_bytes(&self) -> u64 {
        self.read_bytes.get()
    }
}
