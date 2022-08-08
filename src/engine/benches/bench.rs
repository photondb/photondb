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
    let map = Map::open(Options::default()).unwrap();
    for i in 0..N {
        put(&map, i);
    }

    c.bench_function("get", |b| {
        b.iter(|| {
            bench_get(&map);
        })
    });

    c.bench_function("put", |b| {
        b.iter(|| {
            bench_put(&map);
        })
    });

    c.bench_function("delete", |b| {
        b.iter(|| {
            bench_delete(&map);
        })
    });

    println!("{:?}", map.stats());
}

criterion_main!(benches);
criterion_group!(benches, bench);
