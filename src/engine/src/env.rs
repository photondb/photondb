pub trait Env {
    type SequentialWriter;
    type PositionalReader;

    async fn open_sequential_writer<P: AsRef<Path>>(&self, path: P) -> Result<Self::SequentialWriter>;

    async fn open_positional_reader<P: AsRef<Path>>(&self, path: P) -> Result<Self::PositionalReader>;
}
