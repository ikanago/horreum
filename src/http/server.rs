use crate::command::Command;
use crate::horreum::Horreum;
use crate::memtable::Entry;
use hyper::{service, Body, Request, Response, Server, StatusCode};
use log::warn;
use std::convert::Infallible;
use std::net;

pub async fn serve(db: &Horreum, port: u16) -> Result<(), hyper::Error> {
    let addr = net::IpAddr::from([127, 0, 0, 1]);
    let addr = net::SocketAddr::new(addr, port);
    let service = service::make_service_fn(move |_| {
        let db = db.clone();
        async move {
            Ok::<_, Infallible>(service::service_fn(move |req| {
                dbg!(&req);
                let db = db.clone();
                async move { handle(&db, req).await }
            }))
        }
    });
    let server = Server::bind(&addr).serve(service);

    if let Err(e) = server.await {
        warn!("{}", e);
        return Err(e);
    }
    Ok(())
}

async fn handle(db: &Horreum, request: Request<Body>) -> Result<Response<Body>, Infallible> {
    if request.uri().path() != "/" {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap());
    }
    let command = match Command::new(request.method(), request.uri().query()) {
        Ok(command) => command,
        Err(err) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("{}", err)))
                .unwrap())
        }
    };
    let message = apply(db, command).await;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(message))
        .unwrap())
}

async fn apply(db: &Horreum, command: Command) -> Vec<u8> {
    match db.apply(command).await {
        Some(entry) => match entry {
            Entry::Value(value) => value,
            Entry::Deleted => b"Deleted".to_vec(),
        },
        None => b"Entry not exist".to_vec(),
    }
}
