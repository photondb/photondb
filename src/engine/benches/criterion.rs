use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;
use rand::{
    distributions::{Distribution, Uniform},
    rngs::ThreadRng,
    thread_rng,
};

const NUM_KEYS: u64 = 10_000_000;

fn get(table: &Table, k: u64) {
    let buf = k.to_be_bytes();
    let key = buf.as_slice();
    table.get(key, |_| {}).unwrap();
}

fn put(table: &Table, k: u64) {
    let buf = k.to_be_bytes();
    let key = buf.as_slice();
    table.put(key, key).unwrap();
}

struct Bench {
    rng: ThreadRng,
    dist: Uniform<u64>,
    table: Table,
}

impl Bench {
    fn open(opts: Options) -> Self {
        Self {
            rng: thread_rng(),
            dist: Uniform::from(0..NUM_KEYS),
            table: Table::open(opts).unwrap(),
        }
    }

    fn setup(&mut self) {
        for k in 0..NUM_KEYS {
            put(&self.table, k);
        }
    }

    fn bench<F>(&mut self, func: F)
    where
        F: Fn(&Table, u64),
    {
        let k = self.dist.sample(&mut self.rng);
        black_box(func(&self.table, k));
    }
}

fn bench(c: &mut Criterion) {
    let mut bench = Bench::open(Options::default());
    bench.setup();
    c.bench_function("get", |b| b.iter(|| bench.bench(get)));
    c.bench_function("put", |b| b.iter(|| bench.bench(put)));
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench
);
criterion_main!(benches);
