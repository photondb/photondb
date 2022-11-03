use core::fmt;

use clap::{Parser, ValueEnum};

mod error;
pub(crate) use error::Result;

mod bench;

#[derive(Parser, Debug, Clone)]
#[clap(about = "Start bench testing")]
pub(crate) struct Args {
    /// Path of db data folder.
    /// default Env::tempdir.
    #[arg(short, long)]
    path: Option<String>,

    /// Size of each key.
    #[arg(short, long, default_value_t = 16)]
    key_size: u64,

    /// Size of each value in fixed distribution // TODO: support unfixed size
    /// value.
    #[arg(short, long, default_value_t = 1000)]
    value_size: u64,

    /// Number of key/values to place in database.
    #[arg(short, long, default_value_t = 1000000)]
    num: u64,

    /// Number of key/value write op should be taken.
    #[arg(short, long, default_value_t = 0)]
    writes: u64,

    /// Number of concurrent threads to run.
    #[arg(short, long, default_value_t = 1)]
    threads: u64,

    /// The operations to be bench(separate with space).
    /// example: `fillseq,readseq[W1]` will fillseq then readseq with 1 warmup.
    #[arg(short, long)]
    benchmarks: Vec<String>,

    /// The store be used to bench.
    #[arg(short, long, default_value_t = StoreType::Photon)]
    store_type: StoreType,

    /// If true, do not destroy the existing database.  If you set this flag and
    /// also specify a benchmark that wants a fresh database, that benchmark
    /// will fail. default: false,
    #[arg(long, default_value_t = false)]
    use_existing_db: bool,

    /// Stats are reported every N operations when this is greater than 0.
    #[arg(long, default_value_t = 0)]
    stats_interval: u64,

    /// Stats's report interval should not great then every N sec when this is
    /// greater than 0.
    #[arg(long, default_value_t = 0)]
    stats_interval_sec: u64,

    /// Base seed for random.
    /// derived from the current time when it's 0.
    #[arg(long, default_value_t = 0)]
    seed_base: u64,

    // Enable collect histogram.
    #[arg(long, default_value_t = false)]
    hist: bool,
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
