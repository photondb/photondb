use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use clap::Parser;
use photondb_engine::tree;
use rand::{
    distributions::{Distribution, Uniform, WeightedIndex},
    rngs::ThreadRng,
};

#[derive(Parser, Debug)]
struct Config {
    #[clap(long, default_value = "1000000")]
    pub num_kvs: u64,
    #[clap(long, default_value = "8")]
    pub key_len: usize,
    #[clap(long, default_value = "100")]
    pub value_len: usize,
    #[clap(long, default_value = "1000000")]
    pub num_ops: u64,
    #[clap(long, default_value = "1")]
    pub get_weight: usize,
    #[clap(long, default_value = "1")]
    pub put_weight: usize,
    #[clap(long, default_value = "1")]
    pub num_threads: usize,
}

struct Bench {
    inner: Arc<Inner>,
}

impl Bench {
    fn new(cfg: Config) -> Self {
        Self {
            inner: Arc::new(Inner::new(cfg)),
        }
    }

    fn run(&self) {
        self.inner.setup();

        let start = Instant::now();
        let mut threads = Vec::new();
        for _ in 0..self.inner.cfg.num_threads {
            let inner = self.inner.clone();
            threads.push(std::thread::spawn(move || {
                inner.bench();
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let elapsed = start.elapsed().as_secs_f64();

        let stats = self.inner.stats.snapshot();
        println!("{:#?}", stats);
        println!("{:#?}", self.inner.cfg);
        println!("{} ops/sec", stats.num_ops as f64 / elapsed);
    }
}

struct Inner {
    cfg: Config,
    table: Table,
    stats: AtomicStats,
}

impl Inner {
    fn new(cfg: Config) -> Self {
        Self {
            cfg,
            table: Table::open(),
            stats: AtomicStats::default(),
        }
    }

    fn setup(&self) {
        let mut kbuf = vec![0; self.cfg.key_len];
        let mut vbuf = vec![0; self.cfg.value_len];
        let mut workload = Workload::new(&self.cfg);
        for k in 0..self.cfg.num_kvs {
            workload.fill_with_num(k, &mut kbuf);
            workload.fill_with_num(k, &mut vbuf);
            self.table.put(&kbuf, &vbuf);
        }
    }

    fn bench(&self) {
        let mut kbuf = vec![0; self.cfg.key_len];
        let mut vbuf = vec![0; self.cfg.value_len];
        let mut workload = Workload::new(&self.cfg);
        while self.stats.num_ops.get() < self.cfg.num_ops {
            let k = workload.rand_num();
            workload.fill_with_num(k, &mut kbuf);
            match workload.rand_op() {
                Op::Get => {
                    self.table.get(&kbuf);
                    self.stats.num_gets.inc();
                }
                Op::Put => {
                    workload.fill_with_num(k, &mut vbuf);
                    self.table.put(&kbuf, &vbuf);
                    self.stats.num_puts.inc();
                }
            }
            self.stats.num_ops.inc();
        }
    }
}

struct Table {
    table: tree::Table,
    sequence: Counter,
}

impl Table {
    fn open() -> Self {
        let table = tree::Table::open(tree::Options::default()).unwrap();
        Self {
            table,
            sequence: Counter::new(0),
        }
    }

    fn get(&self, k: &[u8]) {
        self.table.get(k, self.sequence.inc(), |_| {}).unwrap();
    }

    fn put(&self, k: &[u8], v: &[u8]) {
        self.table.put(k, self.sequence.inc(), v).unwrap();
    }
}

#[derive(Copy, Clone)]
enum Op {
    Get,
    Put,
}

struct Workload {
    rng: ThreadRng,
    kv_dist: Uniform<u64>,
    op_dist: WeightedIndex<usize>,
    op_choices: [Op; 2],
}

impl Workload {
    fn new(cfg: &Config) -> Self {
        let rng = rand::thread_rng();
        let kv_dist = Uniform::from(0..cfg.num_kvs);
        let op_dist = WeightedIndex::new([cfg.get_weight, cfg.put_weight]).unwrap();
        let op_choices = [Op::Get, Op::Put];
        Self {
            rng,
            kv_dist,
            op_dist,
            op_choices,
        }
    }

    fn rand_op(&mut self) -> Op {
        self.op_choices[self.op_dist.sample(&mut self.rng)]
    }

    fn rand_num(&mut self) -> u64 {
        self.kv_dist.sample(&mut self.rng)
    }

    fn fill_with_num(&mut self, k: u64, buf: &mut [u8]) {
        buf[0..8].copy_from_slice(&k.to_be_bytes());
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct Stats {
    num_ops: u64,
    num_gets: u64,
    num_puts: u64,
}

#[derive(Default)]
struct AtomicStats {
    num_ops: Counter,
    num_gets: Counter,
    num_puts: Counter,
}

impl AtomicStats {
    fn snapshot(&self) -> Stats {
        Stats {
            num_ops: self.num_ops.get(),
            num_gets: self.num_gets.get(),
            num_puts: self.num_puts.get(),
        }
    }
}

struct Counter(AtomicU64);

impl Default for Counter {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Counter {
    const fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    fn inc(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

fn main() {
    let cfg = Config::parse();
    let bench = Bench::new(cfg);
    bench.run();
}
