use criterion::{criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;

const N: usize = 10_000_000;
const M: usize = 10;
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

fn delete(table: &Table, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    table.delete(key, 0).unwrap();
}

fn bench_get(table: &Table) {
    for i in (0..N).step_by(STEP) {
        get(table, i);
    }
}

fn bench_put(table: &Table) {
    for i in (0..N).step_by(STEP) {
        put(table, i);
    }
}

fn bench_delete(table: &Table) {
    for i in (0..N).step_by(STEP) {
        delete(table, i);
    }
}

fn bench(c: &mut Criterion) {
    let table = Table::open(Options::default()).unwrap();
    for i in 0..N {
        put(&table, i);
    }

    c.bench_function("get", |b| b.iter(|| bench_get(&table)));
    c.bench_function("put", |b| b.iter(|| bench_put(&table)));
    c.bench_function("delete", |b| b.iter(|| bench_delete(&table)));

    println!("{:?}", table.stats());
}

criterion_main!(benches);
criterion_group!(benches, bench);
