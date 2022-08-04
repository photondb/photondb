use criterion::{criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;

const N: usize = 1 << 24;
const M: usize = 1 << 14;

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

fn table_delete(table: &Table, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    table.delete(key, 0).unwrap();
}

fn bench_get(table: &Table) {
    for i in (0..N).step_by(M) {
        table_get(table, i);
    }
}

fn bench_put(table: &Table) {
    for i in (0..N).step_by(M) {
        table_put(table, i);
    }
}

fn bench_delete(table: &Table) {
    for i in (0..N).step_by(M) {
        table_delete(table, i);
    }
}

fn bench(c: &mut Criterion) {
    let opts = Options::default();
    let table = Table::open(opts).unwrap();
    for i in 0..N {
        table_put(&table, i);
    }

    let mut num_gets = 0;
    c.bench_function("get", |b| {
        b.iter(|| {
            num_gets += N / M;
            bench_get(&table);
        })
    });

    let mut num_puts = 0;
    c.bench_function("put", |b| {
        b.iter(|| {
            num_puts += N / M;
            bench_put(&table);
        })
    });

    let mut num_deletes = 0;
    c.bench_function("delete", |b| {
        b.iter(|| {
            num_deletes += N / M;
            bench_delete(&table);
        })
    });

    println!(
        "num_gets: {}, num_puts: {}, num_deletes: {}",
        num_gets, num_puts, num_deletes
    );
    println!("{:?}", table.stats());
}

criterion_group!(benches, bench);
criterion_main!(benches);
