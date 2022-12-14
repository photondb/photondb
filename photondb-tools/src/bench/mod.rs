use core::fmt;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};

mod error;
pub(crate) use error::Result;
use photondb::env::Env;

use self::{store::*, workloads::Workloads};

mod store;
mod util;
mod workloads;

#[derive(Parser, Debug, Clone)]
#[clap(about = "Start bench testing")]
pub(crate) struct Args {
    /// Path of db data folder.
    #[arg(long, required = true)]
    db: PathBuf,

    /// Time in seconds for the random-ops tests to run.
    /// When 0 then num & reads determine the test duration
    #[arg(short, long, default_value_t = 0)]
    duration: u64,

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

    /// Number of key read+write pairs op should be taken.
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
    #[arg(long, default_value_t = 0)]
    use_existing_db: u8,

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

    /// Enable collect photondb stats.
    #[arg(long, default_value_t = false)]
    db_stats: bool,

    /// Distribution of generate key.
    /// When do random workload (i.e. readrandom, writerandom..)
    #[arg(long, default_value_t = RandomDistType::Uniform)]
    key_rand_dist: RandomDistType,

    /// Size for tree page size in bytes.
    #[arg(long, default_value_t = 8192)]
    page_size: u64,

    /// Size for read page cache.
    #[arg(long, default_value_t = 134217728)]
    cache_size: u64,

    /// The space watermark which the DB needed to reclaim
    #[clap(long, default_value_t = 10 << 30)]
    space_used_high: u64,

    /// Size for write buffer.
    #[arg(long, default_value_t = 134217728)]
    write_buffer_size: u64,

    /// Does verify checksum.
    #[arg(long, default_value_t = 1)]
    verify_checksum: u8,

    /// Enable compression or not.
    #[arg(long, default_value_t = false)]
    enable_compression: bool,

    /// Does report error when no enough memory.
    #[arg(long, default_value_t = false)]
    cache_strict_capacity_limit: bool,
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
    Flush,
    WaitForReclaiming,
}

impl BenchmarkType {
    pub(crate) fn is_background_job(&self) -> bool {
        matches!(
            self,
            BenchmarkType::Flush | BenchmarkType::WaitForReclaiming
        )
    }
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
            "flush" => BenchmarkType::Flush,
            "waitforreclaiming" => BenchmarkType::WaitForReclaiming,
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum RandomDistType {
    Uniform,
    Zipf,
}

impl fmt::Display for RandomDistType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RandomDistType::Uniform => "uniform",
            RandomDistType::Zipf => "zipf",
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
    use photondb::env::Std as BenchEnv;
    match config.store_type {
        StoreType::Photon => run_store::<PhotondbStore<BenchEnv>, BenchEnv>(config, BenchEnv).await,
    }
}

async fn run_store<S: Store<E>, E: Env>(config: Args, env: E) -> Result<()> {
    let (config, env) = (config.to_owned(), env);
    let mut bench = Workloads::<S, E>::prepare(config, env).await;
    bench.execute().await
}
