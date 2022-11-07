use std::{
    cell::{Ref, RefCell},
    collections::{hash_map, HashMap},
    io::Write,
    rc::Rc,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
    time::{self, Instant, SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use futures::Future;
use photondb::{
    env::{Env, Photon},
    raw::Table,
    TableOptions,
};
use rand::{distributions::Uniform, prelude::Distribution, rngs::SmallRng, RngCore, SeedableRng};
use regex::Regex;

use super::*;
use crate::bench::Result;

pub(crate) struct Benchmark {
    config: Args,
}

impl Benchmark {
    pub(crate) fn new(config: super::Args) -> Self {
        Self { config }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        match self.config.store_type {
            StoreType::Photon => self.run_photon().await,
        }
    }

    async fn run_photon(&self) -> Result<()> {
        let (config, env) = (self.config.to_owned(), Photon);
        let mut bench = PhotonBench::prepare(config, env).await;
        bench.execute().await
    }
}

struct PhotonBench {
    config: Arc<Args>,
    env: Photon,
    table: Option<Table<Photon>>,
    bench_ops: Vec<BenchOperation>,
}

impl PhotonBench {
    async fn prepare(config: Args, env: Photon) -> Self {
        let config = Self::process_config(config);
        let table = Some(Self::open_table(config.to_owned(), &env).await);
        let bench_ops = Self::parse_bench_ops(&config.benchmarks);
        Self {
            config,
            env,
            table,
            bench_ops,
        }
    }

    async fn execute(&mut self) -> Result<()> {
        for bench_op in std::mem::take(&mut self.bench_ops) {
            self.warmup(&bench_op).await?;
            self.do_test(&bench_op).await?;
        }
        self.cleanup().await;
        Ok(())
    }

    fn process_config(mut config: Args) -> Arc<Args> {
        assert!(
            config.key_size >= config.key_prefix_size,
            "prefix_size should not longer than key_size"
        );
        if config.seed_base == 0 {
            config.seed_base = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            println!(
                "Set base_seed to {}, because base_seed is 0",
                config.seed_base
            )
        }
        Arc::new(config)
    }

    async fn open_table(config: Arc<Args>, env: &Photon) -> Table<Photon> {
        let options = TableOptions::default();
        let path = config
            .path
            .as_ref()
            .map(Into::into)
            .unwrap_or_else(std::env::temp_dir);
        Table::open(env.to_owned(), path, options)
            .await
            .expect("open table fail")
    }

    fn parse_bench_ops(benchmark_strs: &str) -> Vec<BenchOperation> {
        let benchmark_strs: Vec<_> = benchmark_strs.split(',').collect();
        let mut benchs = Vec::new();
        let reg = Regex::new("\\[(?P<ctl>.*)\\]").unwrap();
        for bench_str in benchmark_strs {
            let mut warmup_count = 0;
            let mut repeat_count = 0;
            let mut bench_str = bench_str;
            if !bench_str.is_empty() {
                if let Some(start) = bench_str.find('[') {
                    if let Some(caps) = reg.captures(bench_str) {
                        if let Some(ctl) = caps.name("ctl") {
                            let ctl = ctl.as_str();
                            if let Some(w) = ctl.strip_prefix('W') {
                                if let Ok(w) = w.parse::<u64>() {
                                    warmup_count = w;
                                }
                            }
                            if let Some(r) = ctl.strip_prefix('X') {
                                if let Ok(r) = r.parse::<u64>() {
                                    repeat_count = r;
                                }
                            }
                        }
                    }
                    bench_str = &bench_str[..start];
                }
            }
            if repeat_count == 0 {
                repeat_count = 1;
            }
            benchs.push(BenchOperation {
                benchmark_type: bench_str.into(),
                warmup_count,
                repeat_count,
            });
        }
        benchs
    }

    async fn warmup(&mut self, op: &BenchOperation) -> Result<()> {
        if op.warmup_count > 0 {
            println!("Warming up benchmark by running {} times", op.warmup_count);
        }
        for _ in 0..op.warmup_count {
            self.exec_op(op, true).await;
        }
        Ok(())
    }

    async fn do_test(&mut self, op: &BenchOperation) -> Result<()> {
        if op.repeat_count > 0 {
            println!("Running benchmark for {} times", op.repeat_count);
        }
        for _ in 0..op.repeat_count {
            let stats = self.exec_op(op, true).await;
            stats.report(op.benchmark_type);
        }
        Ok(())
    }

    async fn exec_op(&mut self, op: &BenchOperation, _warmup: bool) -> Stats {
        let thread_num = self.config.threads;
        assert!(thread_num > 0);
        let barrier = Barrier::new(thread_num);
        let mut handles = Vec::with_capacity(thread_num as usize);
        let mut ctxs = Vec::with_capacity(thread_num as usize);
        for tid in 0..thread_num as u32 {
            let task_ctx = TaskCtx {
                config: self.config.to_owned(),
                table: self.table.as_ref().unwrap().clone(),
                stats: Rc::new(RefCell::new(Stats::start(
                    tid,
                    self.config.to_owned(),
                    self.table.as_ref().unwrap().to_owned(),
                ))),
                _barrier: barrier.clone(),
                op: op.to_owned(),
                seed: self.config.seed_base + (tid as u64),
            };
            ctxs.push(task_ctx.to_owned());

            let handle = self.env.spawn_background(async move {
                // FIXME: await barrier seems let all task joined...
                // task_ctx.barrier.clone().await;
                let mut task_ctx = task_ctx;
                match task_ctx.op.benchmark_type {
                    BenchmarkType::FillRandom => {
                        Self::do_write(&mut task_ctx, GenMode::Random).await
                    }
                    BenchmarkType::Fillseq => {
                        Self::do_write(&mut task_ctx, GenMode::Sequence).await
                    }
                    BenchmarkType::ReadRandom => Self::do_read_random(&mut task_ctx).await,
                    _ => unimplemented!(),
                }
                task_ctx.stats.as_ref().borrow_mut().stop();
            });
            handles.push(handle);
        }

        self.env
            .spawn_background(async {
                for handle in handles {
                    handle.await;
                }
            })
            .await;

        let mut op_stats = ctxs[0].stats.as_ref().borrow().to_owned();
        for ctx in &ctxs[1..] {
            let stats = ctx.stats.as_ref().borrow();
            op_stats.merge(stats)
        }

        op_stats
    }

    async fn cleanup(&mut self) {
        if let Some(_table) = self.table.take() {
            // let _ = table.close().await;
        }
    }
}

impl PhotonBench {
    async fn do_write(ctx: &mut TaskCtx, mode: GenMode) {
        let table = ctx.table.clone();
        let cfg = ctx.config.to_owned();
        let op_cnt = if cfg.writes >= 0 {
            cfg.writes as u64
        } else {
            cfg.num
        };
        let mut key_gen = KeyGenerator::new(mode, ctx.config.key_size, ctx.config.num, ctx.seed);
        let mut value_gen = ValueGenerator::new(
            ctx.config.value_size_distribution_type,
            ctx.config.value_size,
        );
        let lsn = 0;
        for _ in 0..op_cnt {
            let mut key = vec![0u8; ctx.config.key_size as usize];
            key_gen.generate_key(&mut key);
            let value = value_gen.generate_value();
            table.put(&key, lsn, value).await.unwrap();

            let bytes = key.len() + value.len() + std::mem::size_of::<u64>();

            ctx.stats
                .borrow_mut()
                .finish_operation(OpType::Write, 1, 0, bytes as u64);
        }
    }

    async fn do_read_random(ctx: &mut TaskCtx) {
        let table = ctx.table.clone();
        let cfg = ctx.config.to_owned();
        let op_cnt = if cfg.reads >= 0 {
            cfg.reads as u64
        } else {
            cfg.num
        };

        let mut key_gen = KeyGenerator::new(
            GenMode::Random,
            ctx.config.key_size,
            ctx.config.num,
            ctx.seed,
        );

        let mut reads = 0;
        let mut founds = 0;

        let lsn = 0;
        for _ in 0..op_cnt {
            let mut key = vec![0u8; ctx.config.key_size as usize];
            key_gen.generate_key(&mut key);
            reads += 1;
            if let Some(v) = table.get(&key, lsn).await.expect("get key fail") {
                founds += 1;
                let bytes = key.len() + v.len() + std::mem::size_of::<u64>();
                ctx.stats
                    .borrow_mut()
                    .finish_operation(OpType::Write, 1, 0, bytes as u64);
            } else {
                ctx.stats
                    .borrow_mut()
                    .finish_operation(OpType::Write, 0, 1, 0);
            }
        }
        let msg = format!("({founds} of {reads} found)");
        ctx.stats.borrow_mut().add_msg(&msg);
    }
}

enum GenMode {
    Random,
    Sequence,
}

#[derive(Clone)]
pub struct TaskCtx {
    stats: Rc<RefCell<Stats>>,
    _barrier: Barrier,
    op: BenchOperation,
    table: Table<Photon>,
    config: Arc<Args>,
    seed: u64,
}

unsafe impl Sync for TaskCtx {}

unsafe impl Send for TaskCtx {}

struct KeyGenerator {
    key_per_prefix: u64,
    prefix_size: u64,
    key_size: u64,
    key_nums: u64,
    state: Option<KeyGeneratorState>,
}

pub enum KeyGeneratorState {
    Random { rng: SmallRng },
    Sequence { last: u64 },
}

impl KeyGenerator {
    fn new(mode: GenMode, key_size: u64, key_nums: u64, seed: u64) -> Self {
        let state = Some(match mode {
            GenMode::Random => KeyGeneratorState::Random {
                rng: SmallRng::seed_from_u64(seed),
            },
            GenMode::Sequence => KeyGeneratorState::Sequence { last: 0 },
        });
        let key_per_prefix = 0;
        let prefix_size = 0;
        Self {
            state,
            key_per_prefix,
            prefix_size,
            key_size,
            key_nums,
        }
    }

    fn generate_key(&mut self, buf: &mut [u8]) {
        let rand_num = match self.state.as_mut().unwrap() {
            KeyGeneratorState::Random { rng } => rng.next_u64(),
            KeyGeneratorState::Sequence { last } => {
                *last += 1;
                *last
            }
        };
        self.generate_from_num(rand_num, buf)
    }

    fn generate_from_num(&self, val: u64, buf: &mut [u8]) {
        let mut pos = 0;
        if self.key_per_prefix > 0 {
            let prefix_cnt = self.key_nums / self.key_per_prefix;
            let prefix = val % prefix_cnt;
            let fill_size = self.prefix_size.min(8) as usize;
            buf[pos as usize..fill_size].copy_from_slice(&prefix.to_le_bytes()[..fill_size]);
            if self.prefix_size > 8 {
                buf[(pos + 8) as usize..(self.prefix_size - 8) as usize].fill(0);
            }
            pos += self.prefix_size;
        }
        let fill_size = (self.key_size - pos).min(8);
        let vals = val.to_be_bytes();
        buf[pos as usize..(pos + fill_size) as usize].copy_from_slice(&vals[..fill_size as usize]);
        pos += fill_size;
        if pos < self.key_size {
            buf[pos as usize..self.key_size as usize].fill(0)
        }
    }
}

struct ValueGenerator {
    distribution_type: ValueSizeDistributionType,
    value_size: u64,

    data: Vec<u8>,
    pos: usize,
}

impl ValueGenerator {
    fn new(distribution_type: ValueSizeDistributionType, value_size: u64) -> Self {
        let mut rng = SmallRng::seed_from_u64(301);
        let mut data = Vec::new();
        while data.len() < value_size as usize {
            let d = {
                let size = 100;
                let mut rand_str = Vec::with_capacity(size);
                let range = Uniform::new(0, 95);
                while rand_str.len() < size {
                    rand_str.push(b' ' + (range.sample(&mut rng) as u8));
                    // ' ' to '~'
                }
                rand_str
            };
            data.extend_from_slice(&d);
        }
        Self {
            data,
            pos: 0,
            distribution_type,
            value_size,
        }
    }

    fn generate_value(&mut self) -> &[u8] {
        let require_len = match self.distribution_type {
            ValueSizeDistributionType::Fixed => self.value_size as usize,
            ValueSizeDistributionType::Uniform => unimplemented!(),
        };
        if self.pos + require_len > self.data.len() {
            self.pos = 0;
        }
        self.pos += require_len;
        &self.data[self.pos - require_len..self.pos]
    }
}

#[derive(Clone)]
pub struct Stats {
    tid: u32,

    config: Arc<Args>,

    start: time::Instant,
    finish: Option<time::Instant>,
    total_sec: u64,

    done_cnt: u64,
    err_cnt: u64,
    bytes: u64,

    last_op_finish: Option<time::Instant>,
    last_report_finish: Option<time::Instant>,
    last_report_done_cnt: u64,
    next_report_cnt: u64,

    hist: HashMap<OpType, hdrhistogram::Histogram<u64>>,

    table: Table<Photon>,

    msg: String,
}

impl Stats {
    fn start(tid: u32, config: Arc<Args>, table: Table<Photon>) -> Self {
        let next_report_cnt = config.stats_interval;
        Self {
            tid,
            config,
            start: Instant::now(),
            next_report_cnt,

            total_sec: 0,
            finish: None,
            done_cnt: 0,
            err_cnt: 0,
            bytes: 0,
            last_op_finish: None,
            last_report_finish: None,
            last_report_done_cnt: 0,
            hist: HashMap::new(),
            table,
            msg: "".to_string(),
        }
    }

    fn finish_operation(&mut self, typ: OpType, done: u64, err: u64, bytes: u64) {
        let now = Instant::now();
        let op_elapsed = now.duration_since(*self.last_op_finish.as_ref().unwrap_or(&self.start));
        self.done_cnt += done;
        self.err_cnt += err;
        self.bytes += bytes;
        self.last_op_finish = Some(now);

        if self.config.hist {
            let t = op_elapsed.as_millis() as u64;
            self.hist
                .entry(typ)
                .or_insert_with(|| {
                    hdrhistogram::Histogram::new_with_bounds(1, 60 * 60 * 1000, 2).unwrap()
                })
                .record(t)
                .expect("duration should be in range");
        }

        if self.done_cnt >= self.next_report_cnt {
            if self.config.stats_interval > 0 {
                let report_elapsed =
                    now.duration_since(*self.last_report_finish.as_ref().unwrap_or(&self.start));
                if self.config.stats_interval_sec == 0
                    || report_elapsed.as_secs() >= self.config.stats_interval_sec
                {
                    let start_elapsed = now.duration_since(self.start);
                    println!(
                        "{} ... thread: {}, ({}, {}) ops and ({}, {}) ops/sec in ({}, {})",
                        Utc::now().to_rfc3339(),
                        self.tid,
                        self.last_report_done_cnt,
                        self.done_cnt,
                        (self.done_cnt.saturating_sub(self.last_report_done_cnt) as f64
                            / report_elapsed.as_secs_f64()),
                        (self.done_cnt as f64 / start_elapsed.as_secs_f64()),
                        report_elapsed.as_millis(),
                        start_elapsed.as_millis(),
                    );
                }
                self.last_report_done_cnt = self.done_cnt;
                self.next_report_cnt = self.last_report_done_cnt + self.config.stats_interval;
                self.last_report_finish = Some(now);
            } else {
                self.next_report_cnt += if self.next_report_cnt < 1000 {
                    100
                } else if self.next_report_cnt < 10000 {
                    500
                } else if self.next_report_cnt < 50000 {
                    5000
                } else if self.next_report_cnt < 100000 {
                    10000
                } else if self.next_report_cnt < 500000 {
                    50000
                } else {
                    100000
                };
                print!("... finished {:30} ops\r", self.done_cnt);
            }
            std::io::stdout().flush().unwrap();
        }
    }

    fn add_msg(&mut self, msg: &str) {
        if msg.is_empty() {
            return;
        }
        self.msg.push(' ');
        self.msg.push_str(msg);
    }

    fn stop(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.start);
        self.finish = Some(now);
        self.total_sec = elapsed.as_secs();
    }

    fn merge(&mut self, o: Ref<Stats>) {
        self.done_cnt += o.done_cnt;
        self.bytes += o.bytes;
        self.total_sec += o.total_sec;
        if self.start < o.start {
            self.start = o.start
        }
        if self.finish > o.finish {
            self.finish = o.finish
        }
        if !o.msg.is_empty() {
            self.msg = o.msg.to_owned();
        }
        if self.config.hist {
            for (typ, histo) in &o.hist {
                match self.hist.entry(typ.to_owned()) {
                    hash_map::Entry::Occupied(mut ent) => {
                        let h1 = ent.get_mut();
                        h1.add(histo).unwrap();
                    }
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(histo.to_owned());
                    }
                }
            }
        }
    }

    fn report(&self, bench: BenchmarkType) {
        if self.finish.is_none() {
            return;
        }
        let elapsed = self.finish.as_ref().unwrap().duration_since(self.start);
        let bytes_rate = ((self.bytes / 1024 / 1024) as f64) / elapsed.as_secs_f64();
        println!(
            "{:12?} : {:11.3} ms/op {} ops/sec, {} sec, {} ops; {} MiB/s {}",
            bench,
            (self.total_sec * 1000000) as f64 / (self.done_cnt as f64),
            (self.done_cnt as f64) / (elapsed.as_secs_f64()),
            elapsed.as_secs_f64(),
            self.done_cnt,
            bytes_rate,
            self.msg,
        );
        let table_stats = self.table.stats();
        println!(
            "table stats: conflict: {:?}, success: {:?}",
            table_stats.conflict, table_stats.success
        );
        if self.config.hist {
            for (op, hist) in &self.hist {
                print!(
                    "{:12?} : p50: {} ms, p95: {} ms, p99: {} ms, p100: {} ms",
                    op,
                    hist.value_at_quantile(50.0),
                    hist.value_at_quantile(95.0),
                    hist.value_at_quantile(99.0),
                    hist.value_at_quantile(100.0),
                )
            }
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum OpType {
    Write,
}

#[derive(Clone)]
pub struct Barrier {
    core: Arc<Mutex<BarrierCore>>,
}

pub struct BarrierCore {
    wakers: HashMap<usize, Waker>,
    done: bool,
    count: u64,
}

impl Barrier {
    fn new(count: u64) -> Self {
        assert!(count > 0);
        let core = Arc::new(Mutex::new(BarrierCore {
            wakers: HashMap::with_capacity(count as usize),
            done: false,
            count,
        }));
        Self { core }
    }
}

impl Future for Barrier {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let mut core = this.core.lock().unwrap();
        if core.done {
            Poll::Ready(())
        } else {
            let id = this as *const Barrier as usize;
            if core.wakers.len() == (core.count - 1) as usize {
                core.done = true;
                for w in core.wakers.values() {
                    w.wake_by_ref();
                }
                Poll::Ready(())
            } else {
                core.wakers.insert(id, cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
