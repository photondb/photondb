pub trait Env {
    type SequentialFile;
    type PositionalFile;

    async fn open_sequential_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::SequentialFile>;

    async fn open_positional_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::PositionalFile>;
}

