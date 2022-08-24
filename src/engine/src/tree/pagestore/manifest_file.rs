use crate::env::{SequentialRead, SequentialWrite};

pub struct ManifestFileReader<R> {
    file: R,
}

impl<R: SequentialRead> ManifestFileReader<R> {}

pub struct ManifestFileWriter<W> {
    file: W,
}

impl<W: SequentialWrite> ManifestFileWriter<W> {}
