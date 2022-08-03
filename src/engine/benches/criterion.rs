use criterion::{criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;

const N: usize = 1 << 10;
const M: usize = 1 << 4;
const STEP: usize = N / M;

fn table_get(table: &Table, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    table.get(key, 0).unwrap().unwrap();
}

fn table_put(table: &Table, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    table.put(key, 0, key).unwrap();
}

fn bench_get(table: &Table) {
    for i in (0..N).step_by(STEP) {
        table_get(table, i);
    }
}

fn bench_put(table: &Table) {
    for i in (0..N).step_by(STEP) {
        table_put(table, i);
    }
}

fn bench(c: &mut Criterion) {
    let opts = Options::default();
    let table = Table::open(opts).unwrap();
    for i in 0..N {
        table_put(&table, i);
    }

    c.bench_function("get", |b| b.iter(|| bench_get(&table)));
    c.bench_function("put", |b| b.iter(|| bench_put(&table)));
}

criterion_group!(benches, bench);
criterion_main!(benches);
