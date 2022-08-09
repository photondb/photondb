use criterion::{criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;

const N: u64 = 10_000_000;
const M: u64 = 1000;
const STEP: usize = (N / M) as usize;

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

fn bench_get(table: &Table) {
    for k in (0..N).step_by(STEP) {
        get(table, k);
    }
}

fn bench_put(table: &Table) {
    for k in (0..N).step_by(STEP) {
        put(table, k);
    }
}

fn bench(c: &mut Criterion) {
    let opts = Options::default();
    let table = Table::open(opts).unwrap();
    for k in 0..N {
        put(&table, k);
    }
    println!("{:?}", table.stats());

    c.bench_function("get", |b| b.iter(|| bench_get(&table)));
    println!("{:?}", table.stats());
    c.bench_function("put", |b| b.iter(|| bench_put(&table)));
    println!("{:?}", table.stats());
}

criterion_main!(benches);
criterion_group!(benches, bench);
