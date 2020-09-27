use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("Put key value pairs");
    group.finish();
}

criterion_group!(benches, bench_put);
criterion_main!(benches);
