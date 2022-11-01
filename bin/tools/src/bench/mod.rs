use core::fmt;

use clap::{Parser, ValueEnum};

mod error;
pub(crate) use error::Result;

mod bench;

#[derive(Parser, Debug, Clone)]
#[clap(about = "Start bench testing")]
pub(crate) struct Args {
    // Path of db data folder.
    // default Env::tempdir.
    #[arg(short, long)]
    path: Option<String>,

    /// Size of each key.
    #[arg(short, long, default_value_t = 16)]
    key_size: u64,

    /// Size of each value in fixed distribution // TODO: support unfixed size
    /// value.
    #[arg(short, long, default_value_t = 100)]
    value_size: u64,

    /// Number of key/values to place in database.
    #[arg(short, long, default_value_t = 1000000)]
    num: u64,

    /// Number of concurrent threads to run.
    #[arg(short, long, default_value_t = 1)]
    threads: u64,

    /// The operations to be bench(separate with comma).
    /// example: `fillseq,readseq[W1]` will fillseq then readseq with 1 warmup.
    #[arg(short, long)]
    benchmarks: Vec<String>,

    /// The store be used to bench.
    #[arg(short, long, default_value_t = StoreType::Photon)]
    store_type: StoreType,

    ///If true, do not destroy the existing database.  If you set this flag and
    /// also specify a benchmark that wants a fresh database, that benchmark
    /// will fail. default: false,
    #[arg(long, default_value_t = false)]
    use_existing_db: bool,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct BenchOperation {
    repeat_count: u64,
    warmup_count: u64,
    benchmark_type: BenchmarkType,
}

#[derive(Debug, Copy, Clone)]
enum BenchmarkType {
    Fillseq,
    FillRandom,
    ReadSeq,
    WriteRandom,
}

impl From<&str> for BenchmarkType {
    fn from(str: &str) -> Self {
        match str {
            "fillseq" => BenchmarkType::Fillseq,
            "fillrandom" => BenchmarkType::FillRandom,
            "readseq" => BenchmarkType::ReadSeq,
            "writerandom" => BenchmarkType::WriteRandom,
            _ => panic!("invalid benchmark type"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum StoreType {
    Photon,
}

impl fmt::Display for StoreType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StoreType::Photon => "photon",
        })
    }
}

pub(crate) async fn run(config: Args) -> Result<()> {
    let mut bench = bench::Benchmark::new(config);
    bench.run().await
}
