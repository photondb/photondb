use core::fmt;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};

mod error;
pub(crate) use error::Result;

mod benchmark;

#[derive(Parser, Debug, Clone)]
#[clap(about = "Start bench testing")]
pub(crate) struct Args {
    /// Path of db data folder.
    #[arg(long, required = true)]
    db: PathBuf,

    /// Size of each key.
    #[arg(short, long, default_value_t = 16)]
    key_size: u64,

    // Controls the key num for per prefix, 0 mean no prefix.
    #[arg(long, default_value_t = 0)]
    keys_per_prefix: u64,

    // Prefix size of the key, it works with `keys_per_prefix`.
    #[arg(long, default_value_t = 0)]
    key_prefix_size: u64,

    /// Size of each value in fixed distribution // TODO: support unfixed size
    /// value.
    #[arg(short, long, default_value_t = 100)]
    value_size: u64,

    #[arg(long, default_value_t = ValueSizeDistributionType::Fixed)]
    value_size_distribution_type: ValueSizeDistributionType,

    /// Number of key/values to place in database.
    #[arg(short, long, default_value_t = 1000000)]
    num: u64,

    /// Number of key/value write op should be taken.
    #[arg(short, long, default_value_t = -1)]
    writes: i64,

    /// Number of key read op should be taken.
    #[arg(short, long, default_value_t = -1)]
    reads: i64,

    /// Number of key read+write paire op should be taken.
    #[arg(long, default_value_t = -1)]
    read_writes: i64,

    /// Ratio of reads to reads/writes for the ReadRandomWriteRandom workload.
    /// Default value 90 means "9 gets for every 1 put".
    #[arg(long, default_value_t = 90)]
    read_write_percent: u32,

    /// Number of concurrent threads to run.
    #[arg(short, long, default_value_t = 1)]
    threads: u64,

    /// The operations to be bench(separate with comma).
    /// example: `fillseq,readseq[W1]` will fillseq then readseq with 1 warmup.
    #[arg(short, long)]
    benchmarks: String,

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

    /// Enable collect histogram.
    #[arg(long, default_value_t = false)]
    hist: bool,

    /// Enable collect photondb table tree stats.
    #[arg(long, default_value_t = false)]
    table_stats: bool,
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
    ReadRandom,
    UpdateRandom,
    ReadRandomWriteRandom,
}

impl From<&str> for BenchmarkType {
    fn from(str: &str) -> Self {
        match str {
            "fillseq" => BenchmarkType::Fillseq,
            "fillrandom" => BenchmarkType::FillRandom,
            "readseq" => BenchmarkType::ReadSeq,
            "readrandom" => BenchmarkType::ReadRandom,
            "updaterandom" => BenchmarkType::UpdateRandom,
            "readrandomwriterandom" => BenchmarkType::ReadRandomWriteRandom,
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

#[derive(ValueEnum, Clone, Debug, Copy)]
enum ValueSizeDistributionType {
    Fixed,
    Uniform,
}

impl fmt::Display for ValueSizeDistributionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ValueSizeDistributionType::Fixed => "fixed",
            ValueSizeDistributionType::Uniform => "uniform",
        })
    }
}

pub(crate) async fn run(config: Args) -> Result<()> {
    let mut bench = benchmark::Benchmark::new(config);
    bench.run().await
}
