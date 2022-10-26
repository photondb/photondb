pub(crate) trait EncodeDecode<'a> {
    fn size(&self) -> usize;

    fn encode_to(&self, buf: &mut &mut [u8]);

    fn decode_from(buf: &mut &'a [u8]) -> Self
    where
        Self: 'a;
}
