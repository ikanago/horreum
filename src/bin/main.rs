use horreum::http;
use horreum::index::Horreum;
use clap::{clap_app, crate_version};

fn main() {
    let matches = clap_app!(horreum =>
        (version: crate_version!())
        (@arg NUM_THREADS: -n +takes_value "Number of threads to handle requests")
        (@arg PORT: -p --port +takes_value "Number of threads to handle requests")
    ).get_matches();
    
    let num_threads = matches.value_of("NUM_THREADS").unwrap_or("1");
    let num_threads = num_threads.parse::<usize>().unwrap();
    let port = matches.value_of("PORT").unwrap_or("8080");
    let port = port.parse::<usize>().unwrap();
    let db = Horreum::new();
    http::listen(&db, num_threads, port);
}
