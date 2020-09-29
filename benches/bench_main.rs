mod benchmarks;
mod setup;

use criterion::criterion_main;

criterion_main! {
    benchmarks::put_parallel::benches,
}
