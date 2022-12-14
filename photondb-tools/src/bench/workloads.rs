use std::{
    cell::RefCell,
    marker::PhantomData,
    rc::Rc,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use photondb::{env::Env, TableStats};
use regex::Regex;

use super::{util::*, *};
use crate::bench::{Args, BenchOperation, BenchmarkType, Result};

pub(super) struct Workloads<S: Store<E>, E: Env> {
    ctx: WorkloadContext,
    config: Arc<Args>,
    env: E,
    table: Option<S>,
    bench_ops: Vec<BenchOperation>,
}

#[derive(Default)]
pub(super) struct WorkloadContext {
    pub(super) last_table_stats: TableStats,
    total_task_offset: u64,
}

impl<S: Store<E>, E: Env> Workloads<S, E> {
    pub(super) async fn prepare(config: Args, env: E) -> Self {
        let config = Self::process_config(config);
        let table = Some(S::open_table(config.clone(), &env).await);
        let bench_ops = Self::parse_bench_ops(&config.benchmarks);
        Self {
            ctx: Default::default(),
            config,
            env,
            table,
            bench_ops,
        }
    }

    pub(super) async fn execute(&mut self) -> Result<()> {
        for bench_op in std::mem::take(&mut self.bench_ops) {
            if !bench_op.benchmark_type.is_background_job() {
                self.warmup(&bench_op).await?;
            }
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
        if config.stats_interval_sec > 0 {
            config.stats_interval = 1000
        }
        Arc::new(config)
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
            let mut stats = self.exec_op(op, true).await;
            stats.report(&mut self.ctx, op.benchmark_type);
        }
        Ok(())
    }

    async fn exec_op(&mut self, op: &BenchOperation, _warmup: bool) -> Stats<S, E> {
        if op.benchmark_type.is_background_job() {
            return self.exec_bg_op(op).await;
        }

        let thread_num = self.config.threads;
        assert!(thread_num > 0);
        let barrier = Barrier::new(thread_num);
        let mut handles = Vec::with_capacity(thread_num as usize);
        let mut ctxs = Vec::with_capacity(thread_num as usize);
        for tid in 0..thread_num as u32 {
            let seed_offset = self.ctx.total_task_offset;
            self.ctx.total_task_offset += 1;
            let task_ctx = TaskCtx {
                config: self.config.to_owned(),
                table: self.table.as_ref().unwrap().clone(),
                stats: Rc::new(RefCell::new(Stats::start(
                    tid,
                    self.config.to_owned(),
                    self.table.to_owned(),
                ))),
                _barrier: barrier.clone(),
                op: op.to_owned(),
                seed: self.config.seed_base + seed_offset,
                _mark: PhantomData,
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
                    BenchmarkType::UpdateRandom => Self::do_update_random(&mut task_ctx).await,
                    BenchmarkType::ReadRandomWriteRandom => {
                        Self::do_read_random_write_random(&mut task_ctx).await
                    }
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

    async fn exec_bg_op(&mut self, op: &BenchOperation) -> Stats<S, E> {
        let stats = Stats::start(0, self.config.to_owned(), self.table.to_owned());
        match op.benchmark_type {
            BenchmarkType::Flush => {
                self.table.as_ref().unwrap().flush().await;
            }
            BenchmarkType::WaitForReclaiming => {
                self.table.as_ref().unwrap().wait_for_reclaiming().await;
            }
            _ => unreachable!(),
        }
        stats
    }

    async fn cleanup(&mut self) {
        if let Some(table) = self.table.take() {
            table.close().await.expect("Only one references here");
        }
    }
}

impl<S: Store<E>, E: Env> Workloads<S, E> {
    async fn do_write(ctx: &mut TaskCtx<S, E>, mode: GenMode) {
        let table = ctx.table.clone();
        let cfg = ctx.config.to_owned();
        let op_cnt = if cfg.writes >= 0 {
            cfg.writes as u64
        } else {
            cfg.num
        };
        let mut key_gen = KeyGenerator::new(
            mode,
            ctx.config.key_size,
            ctx.config.num,
            ctx.seed,
            ctx.config.key_rand_dist,
        );
        let mut value_gen = ValueGenerator::new(
            ctx.config.value_size_distribution_type,
            ctx.config.value_size,
        );
        for _ in Until::new(op_cnt, cfg.duration) {
            let mut key = vec![0u8; ctx.config.key_size as usize];
            key_gen.generate_key(&mut key);
            let value = value_gen.generate_value();
            table.put(&key, 0, value).await.unwrap();

            photonio::task::yield_now().await;

            let bytes = key.len() + value.len() + std::mem::size_of::<u64>();

            ctx.stats
                .borrow_mut()
                .finish_operation(OpType::Write, 1, 0, bytes as u64);
        }
    }

    async fn do_read_random(ctx: &mut TaskCtx<S, E>) {
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
            ctx.config.key_rand_dist,
        );

        let mut reads = 0;
        let mut founds = 0;

        for _ in Until::new(op_cnt, cfg.duration) {
            let mut key = vec![0u8; ctx.config.key_size as usize];
            key_gen.generate_key(&mut key);
            reads += 1;
            if let Some(v) = table.get(&key, 0).await.expect("get key fail") {
                founds += 1;
                let bytes = key.len() + v.len() + std::mem::size_of::<u64>();
                ctx.stats
                    .borrow_mut()
                    .finish_operation(OpType::Read, 1, 0, bytes as u64);
            } else {
                ctx.stats
                    .borrow_mut()
                    .finish_operation(OpType::Read, 0, 1, 0);
            }
        }
        let msg = format!("(reads:{reads} founds:{founds})");
        ctx.stats.borrow_mut().add_msg(&msg);
    }

    async fn do_update_random(ctx: &mut TaskCtx<S, E>) {
        let table = ctx.table.clone();
        let cfg = ctx.config.to_owned();
        let op_cnt = if cfg.read_writes >= 0 {
            cfg.read_writes as u64
        } else {
            cfg.num
        };

        let mut updates = 0;
        let mut founds = 0;

        let mut key_gen = KeyGenerator::new(
            GenMode::Random,
            ctx.config.key_size,
            ctx.config.num,
            ctx.seed,
            ctx.config.key_rand_dist,
        );
        let mut val_gen = ValueGenerator::new(
            ctx.config.value_size_distribution_type,
            ctx.config.value_size,
        );

        for _ in Until::new(op_cnt, cfg.duration) {
            let mut bytes = 0;
            let mut key = vec![0u8; ctx.config.key_size as usize];
            key_gen.generate_key(&mut key);

            if let Some(ov) = table.get(&key, 0).await.expect("read of update fail") {
                founds += 1;
                bytes += key.len() + ov.len() + std::mem::size_of::<u64>();
            }

            updates += 1;
            let value = val_gen.generate_value();
            table.put(&key, 0, value).await.expect("put of update fail");

            photonio::task::yield_now().await;

            bytes += key.len() + value.len() + std::mem::size_of::<u64>();

            ctx.stats
                .borrow_mut()
                .finish_operation(OpType::Update, 1, 0, bytes as u64);
        }
        let msg = format!("(updates:{updates} founds:{founds})");
        ctx.stats.borrow_mut().add_msg(&msg);
    }

    async fn do_read_random_write_random(ctx: &mut TaskCtx<S, E>) {
        let table = ctx.table.clone();
        let cfg = ctx.config.to_owned();
        let op_cnt = cfg.num;

        let mut key_gen = KeyGenerator::new(
            GenMode::Random,
            ctx.config.key_size,
            ctx.config.num,
            ctx.seed,
            ctx.config.key_rand_dist,
        );
        let mut val_gen = ValueGenerator::new(
            ctx.config.value_size_distribution_type,
            ctx.config.value_size,
        );

        let mut reads = 0;
        let mut writes = 0;
        let mut founds = 0;

        let mut read_weight = 0;
        let mut write_weight = 0;
        for _ in Until::new(op_cnt, cfg.duration) {
            if read_weight == 0 && write_weight == 0 {
                read_weight = cfg.read_write_percent;
                write_weight = 100 - read_weight;
            }

            let mut key = vec![0u8; ctx.config.key_size as usize];
            key_gen.generate_key(&mut key);

            if read_weight > 0 {
                if let Some(v) = table.get(&key, 0).await.expect("get key fail") {
                    founds += 1;
                    let bytes = key.len() + v.len() + std::mem::size_of::<u64>();
                    ctx.stats
                        .borrow_mut()
                        .finish_operation(OpType::Read, 1, 0, bytes as u64);
                } else {
                    ctx.stats
                        .borrow_mut()
                        .finish_operation(OpType::Read, 0, 1, 0);
                }

                read_weight -= 1;
                reads += 1;
                continue;
            }

            if write_weight > 0 {
                let value = val_gen.generate_value();

                table.put(&key, 0, value).await.unwrap();

                photonio::task::yield_now().await;

                let bytes = key.len() + value.len() + std::mem::size_of::<u64>();

                ctx.stats
                    .borrow_mut()
                    .finish_operation(OpType::Write, 1, 0, bytes as u64);

                write_weight -= 1;
                writes += 1;
            }
        }
        let msg = format!(
            "(reads:{reads} writes:{writes} total:{} founds:{founds})",
            reads + writes
        );
        ctx.stats.borrow_mut().add_msg(&msg);
    }
}

#[derive(Clone)]
pub(crate) struct TaskCtx<S: Store<E>, E: Env> {
    stats: Rc<RefCell<Stats<S, E>>>,
    _barrier: Barrier,
    op: BenchOperation,
    table: S,
    config: Arc<Args>,
    seed: u64,
    _mark: PhantomData<E>,
}

unsafe impl<S: Store<E>, E: Env> Sync for TaskCtx<S, E> {}

unsafe impl<S: Store<E>, E: Env> Send for TaskCtx<S, E> {}
