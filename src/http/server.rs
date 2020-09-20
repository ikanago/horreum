use crate::index::Horreum;
use crossbeam;
use log::warn;
use std::io;
use std::sync::Arc;
use tiny_http::{Method, Request, Response, Server};

pub fn listen(db: &Horreum) {
    let server = Arc::new(Server::http("127.0.0.1:8080").unwrap());
    let thread_num = 4;

    crossbeam::scope(|s| {
        for i in 0..thread_num {
            let server = server.clone();
            s.spawn(move |_| {
                for request in server.incoming_requests() {
                    println!("{}", i);
                    handle(request, db);
                }
            });
        }
    })
    .unwrap();
}

fn handle(request: Request, db: &Horreum) {
    let response = match request.method() {
        Method::Get => get(db, &request),
        Method::Post => put(db, &request),
        Method::Delete => delete(db, &request),
        _ => return,
    };
    dbg!(&request.url());
    if let Err(err) = request.respond(response) {
        warn!("{}", err);
    }
}

fn get(db: &Horreum, request: &Request) -> Response<io::Cursor<Vec<u8>>> {
    tiny_http::Response::from_string("Get")
}

fn put(db: &Horreum, request: &Request) -> Response<io::Cursor<Vec<u8>>> {
    tiny_http::Response::from_string("Put")
}

fn delete(db: &Horreum, request: &Request) -> Response<io::Cursor<Vec<u8>>> {
    tiny_http::Response::from_string("Delete")
}
