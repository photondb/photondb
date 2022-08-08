use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;

const T: usize = 4;
const N: usize = 10_000_000;
const M: usize = 1000_000;
const STEP: usize = N / M;

fn get(table: &Table, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    table.get(key, 0, |_| {}).unwrap();
}

fn put(table: &Table, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    table.put(key, 0, key).unwrap();
}

fn concurrent_get(table: &Table) {
    let mut handles = Vec::new();
    for _ in 0..T {
        let table = table.clone();
        let handle = std::thread::spawn(move || {
            for i in (0..N).step_by(STEP) {
                get(&table, i);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

fn concurrent_put(table: &Table) {
    let mut handles = Vec::new();
    for _ in 0..T {
        let table = table.clone();
        let handle = std::thread::spawn(move || {
            for i in (0..N).step_by(STEP) {
                put(&table, i);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

fn bench_get(table: &Table, iters: u64) -> Duration {
    let start = Instant::now();
    for _ in 0..iters {
        concurrent_get(table);
    }
    start.elapsed()
}

fn bench_put(table: &Table, iters: u64) -> Duration {
    let start = Instant::now();
    for _ in 0..iters {
        concurrent_put(table);
    }
    start.elapsed()
}

fn bench(c: &mut Criterion) {
    let opts = Options::default();
    let table = Table::open(opts).unwrap();
    for i in 0..N {
        put(&table, i);
    }

    c.bench_function("get", |b| b.iter_custom(|iters| bench_get(&table, iters)));
    c.bench_function("put", |b| b.iter_custom(|iters| bench_put(&table, iters)));

    println!("{:?}", table.stats());
}

criterion_main!(benches);
criterion_group!(benches, bench);
