use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;

const NUM_KEYS: u64 = 10_000_000;
const NUM_STEPS: u64 = 1000;
const NUM_OPS_PER_STEP: u64 = 100;
const STEP: usize = (NUM_KEYS / NUM_STEPS) as usize;
const NUM_THREADS: usize = 4;

fn get(table: &Table, k: u64) {
    let buf = k.to_be_bytes();
    let key = buf.as_slice();
    table.get(key, 0, |_| {}).unwrap();
}

fn put(table: &Table, k: u64) {
    let buf = k.to_be_bytes();
    let key = buf.as_slice();
    table.put(key, 0, key).unwrap();
}

fn bench_function<F>(table: &Table, func: F)
where
    F: Fn(&Table, u64),
{
    for k in (0..NUM_KEYS).step_by(STEP) {
        for i in 0..NUM_OPS_PER_STEP {
            func(table, k + i);
        }
    }
}

fn custom_bench_function<F>(iters: u64, table: &Table, func: F) -> Duration
where
    F: Fn(&Table, u64) + Copy + Send + 'static,
{
    let start = Instant::now();
    for _ in 0..iters {
        let mut handles = Vec::new();
        for _ in 0..NUM_THREADS {
            let table = table.clone();
            let handle = std::thread::spawn(move || bench_function(&table, func));
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
    start.elapsed()
}

fn bench(c: &mut Criterion) {
    let opts = Options::default();
    let table = Table::open(opts).unwrap();
    for k in 0..NUM_KEYS {
        put(&table, k);
    }
    println!("{:?}", table.stats());

    c.bench_function("get", |b| {
        b.iter_custom(|iters| custom_bench_function(iters, &table, get))
    });
    // println!("Get {:?}", table.stats());
    c.bench_function("put", |b| {
        b.iter_custom(|iters| custom_bench_function(iters, &table, put))
    });
    // println!("Put {:?}", table.stats());
}

criterion_main!(benches);
criterion_group!(benches, bench);
