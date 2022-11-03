use std::{path::Path, sync::Arc};

use photondb::{
    env::{Env, Photon},
    raw::Table,
};

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

struct PhotonBench<E: Env> {
    config: Args,
    table: Option<Table<E>>,
    bench_ops: Vec<BenchOperation>,
}

impl<E: Env> PhotonBench<E> {
    async fn prepare(config: Args, env: E) -> Self {
        let options = photondb::Options::default();
        let path = config
            .path
            .as_ref()
            .map(Into::into)
            .unwrap_or_else(|| std::env::temp_dir());
        let table = Some(
            Table::open(env, path, options)
                .await
                .expect("open table fail"),
        );
        let benchmarks = Self::parse_bench_ops(config.benchmarks.as_slice());
        Self {
            config,
            table,
            bench_ops: benchmarks,
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

    fn parse_bench_ops(benchmark_strs: &[String]) -> Vec<BenchOperation> {
        let mut benchs = Vec::new();
        for bench_str in benchmark_strs {
            benchs.push(BenchOperation {
                benchmark_type: bench_str.as_str().into(),
                warmup_count: 0, // TODO: support "[X11]" and "[W1231]"
                repeat_count: 0,
            });
        }
        benchs
    }

    async fn warmup(&mut self, op: &BenchOperation) -> Result<()> {
        let mut warmup_count = op.warmup_count;
        if warmup_count > 0 {
            println!("Warming up benchmark by running {warmup_count} times");
        }
        while warmup_count > 0 {
            warmup_count -= 1;
        }
        Ok(())
    }

    async fn do_test(&mut self, op: &BenchOperation) -> Result<()> {
        let mut repeat = op.repeat_count;
        if repeat > 0 {
            println!("Running benchmark for {repeat} times");
        }
        while repeat > 0 {
            repeat -= 1;
        }
        Ok(())
    }

    async fn cleanup(&mut self) {
        if let Some(table) = self.table.take() {
            let _ = table.close().await;
        }
    }
}
