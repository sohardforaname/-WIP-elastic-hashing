use criterion::{criterion_group, criterion_main, Criterion};
use hashing::bucket::ElasticHashMap;

fn bench_insert(c: &mut Criterion) {
    c.bench_function("insert 100000 items", |b| {
        b.iter(|| {
            let mut map = ElasticHashMap::<i32, i32>::with_capacity(200000);
            for i in 0..100000 {
                map.insert(i, i);
            }
        })
    });
}

fn bench_insert_std_hashmap(c: &mut Criterion) {
    c.bench_function("insert 100000 items std hashmap", |b| {
        b.iter(|| {
            let mut map = std::collections::HashMap::<i32, i32>::with_capacity(200000);
            for i in 0..100000 {
                map.insert(i, i);
            }
        })
    });
}

criterion_group!(benches, bench_insert, bench_insert_std_hashmap);
criterion_main!(benches);
