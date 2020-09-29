use crate::setup::{launch_db, COUNT, PAIRS};
use criterion::{criterion_group, BenchmarkId, Criterion};
use futures::Future;
use reqwest::Client;
use tokio::runtime::Runtime;

async fn put_pairs(client: &Client, port: usize) {
    let futures = futures::future::join_all(PAIRS.iter().map(|(key, value)| {
        let client = client.clone();
        tokio::spawn(async move {
            let url = format!("http://localhost:{}/?key={}&value={}", port, key, value);
            if let Err(err) = client.post(&url).send().await {
                panic!("{}", err);
            }
        })
    }));
    futures.await;
}

pub fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("Put key value pairs in parallel");
    // let db_with_one_thread = launch_db(1, 8081);
    // let db_with_more_threads = launch_db(8, 8082);
    let client = Client::new();

    group.bench_function("put_pairs_to_one_threaded()", |b| {
        b.iter(|| {
            let mut rt = Runtime::new().unwrap();
            rt.block_on(put_pairs(&client, 8081));
        });
    });
    group.bench_function("put_pairs_to_multi_threaded()", |b| {
        b.iter(|| {
            let mut rt = Runtime::new().unwrap();
            rt.block_on(put_pairs(&client, 8082))
        });
    });
    group.finish();
}

criterion_group!(benches, bench_put);
