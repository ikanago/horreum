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
                async move { handle(req, &db).await }
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

async fn handle(request: Request<Body>, db: &Horreum) -> Result<Response<Body>, hyper::Error> {
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

async fn apply(db: &Horreum, command: Command) -> String {
    match db.apply(command).await {
        Some(entry) => match entry {
            Entry::Value(value) => String::from_utf8(value).unwrap(),
            Entry::Deleted => "Deleted".to_string(),
        },
        None => "Entry not exist".to_string(),
    }
}
