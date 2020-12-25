use crate::command::Command;
use crate::memtable::Entry;
use crate::Message;
use hyper::server::Server;
use hyper::{service, Body, Request, Response, StatusCode};
use log::warn;
use std::convert::Infallible;
use std::net;
use tokio::sync::mpsc;

pub async fn serve(port: u16, memtable_tx: mpsc::Sender<Message>) -> Result<(), hyper::Error> {
    let addr = net::IpAddr::from([127, 0, 0, 1]);
    let addr = net::SocketAddr::new(addr, port);
    let handler = Handler::new(memtable_tx);
    let service = service::make_service_fn(move |_| {
        let handler = handler.clone();
        async move {
            Ok::<_, Infallible>(service::service_fn(move |req| {
                dbg!(&req);
                let handler = handler.clone();
                async move { handler.handle(req).await }
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

#[derive(Clone)]
pub struct Handler {
    memtable_tx: mpsc::Sender<Message>,
}

impl Handler {
    fn new(memtable_tx: mpsc::Sender<Message>) -> Self {
        Self { memtable_tx }
    }

    async fn handle(&self, request: Request<Body>) -> Result<Response<Body>, Infallible> {
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
        let response = self.apply(command).await;
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(response))
            .unwrap())
    }

    async fn apply(&self, command: Command) -> Vec<u8> {
        let (tx, mut rx) = mpsc::channel(1);
        self.memtable_tx.send((command, tx)).await.unwrap();
        let entry = rx.recv().await.unwrap();
        match entry {
            Some(entry) => match entry {
                Entry::Value(value) => value,
                Entry::Deleted => b"Deleted".to_vec(),
            },
            None => b"Entry not exist".to_vec(),
        }
    }
}
