use criterion::{criterion_group, criterion_main, Criterion};
use photondb_engine::tree::*;

const N: usize = 10_000_000;
const M: usize = 10;
const STEP: usize = N / M;

fn get(map: &Map, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    map.get(key, 0, |_| {}).unwrap();
}

fn put(map: &Map, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    map.put(key, 0, key).unwrap();
}

fn delete(map: &Map, i: usize) {
    let buf = i.to_be_bytes();
    let key = buf.as_slice();
    map.delete(key, 0).unwrap();
}

fn bench_get(map: &Map) {
    for i in (0..N).step_by(STEP) {
        get(map, i);
    }
}

fn bench_put(map: &Map) {
    for i in (0..N).step_by(STEP) {
        put(map, i);
    }
}

fn bench_delete(map: &Map) {
    for i in (0..N).step_by(STEP) {
        delete(map, i);
    }
}

fn bench(c: &mut Criterion) {
    let opts = Options::default();
    let map = Map::open(opts).unwrap();
    for i in 0..N {
        put(&map, i);
    }

    let mut num_gets = 0;
    c.bench_function("get", |b| {
        b.iter(|| {
            num_gets += M;
            bench_get(&map);
        })
    });

    let mut num_puts = 0;
    c.bench_function("put", |b| {
        b.iter(|| {
            num_puts += M;
            bench_put(&map);
        })
    });

    let mut num_deletes = 0;
    c.bench_function("delete", |b| {
        b.iter(|| {
            num_deletes += M;
            bench_delete(&map);
        })
    });

    println!(
        "num_gets: {}, num_puts: {}, num_deletes: {}",
        num_gets, num_puts, num_deletes
    );
    println!("{:?}", map.stats());
}

criterion_group!(benches, bench);
criterion_main!(benches);
