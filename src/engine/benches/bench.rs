use criterion::{criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;

const NUM_KEYS: u64 = 10_000_000;
const NUM_STEPS: u64 = 100;
const NUM_OPS_PER_STEP: u64 = 100;
const STEP: usize = (NUM_KEYS / NUM_STEPS) as usize;

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

fn bench(c: &mut Criterion) {
    let opts = Options::default();
    let table = Table::open(opts).unwrap();
    for k in 0..NUM_KEYS {
        put(&table, k);
    }
    println!("{:?}", table.stats());

    c.bench_function("get", |b| b.iter(|| bench_function(&table, get)));
    // println!("Get {:?}", table.stats());
    c.bench_function("put", |b| b.iter(|| bench_function(&table, put)));
    // println!("Put {:?}", table.stats());
}

criterion_main!(benches);
criterion_group!(benches, bench);
