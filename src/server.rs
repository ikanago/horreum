use log::warn;
use std::io;
use std::sync::Arc;
use std::thread;
use tiny_http::{Method, Request, Response, Server};

pub fn listen() {
    let server = Arc::new(Server::http("127.0.0.1:8080").unwrap());
    let thread_num = 4;
    let mut handles = Vec::new();

    for _ in 0..thread_num {
        let server = server.clone();
        handles.push(thread::spawn(move || {
            for request in server.incoming_requests() {
                handle(request);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn handle(request: Request) {
    let response = match request.method() {
        Method::Get => get(),
        Method::Post => put(),
        Method::Delete => delete(),
        _ => return,
    };
    if let Err(err) = request.respond(response) {
        warn!("{}", err);
    }
}

fn get() -> Response<io::Cursor<Vec<u8>>> {
    tiny_http::Response::from_string("Get")
}

fn put() -> Response<io::Cursor<Vec<u8>>> {
    tiny_http::Response::from_string("Put")
}

fn delete() -> Response<io::Cursor<Vec<u8>>> {
    tiny_http::Response::from_string("Delete")
}
