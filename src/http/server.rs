use crate::command::Command;
use crate::Message;
use hyper::server::Server;
use hyper::{service, Body, Request, Response, StatusCode};
use log::warn;
use std::convert::Infallible;
use std::net;
use tokio::sync::mpsc;

pub async fn serve(
    port: u16,
    memtable_tx: mpsc::Sender<Message>,
    sstable_tx: mpsc::Sender<Message>,
) -> Result<(), hyper::Error> {
    let addr = net::IpAddr::from([127, 0, 0, 1]);
    let addr = net::SocketAddr::new(addr, port);
    let handler = Handler::new(memtable_tx, sstable_tx);
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
pub(crate) struct Handler {
    memtable_tx: mpsc::Sender<Message>,
    sstable_tx: mpsc::Sender<Message>,
}

impl Handler {
    pub(crate) fn new(
        memtable_tx: mpsc::Sender<Message>,
        sstable_tx: mpsc::Sender<Message>,
    ) -> Self {
        Self {
            memtable_tx,
            sstable_tx,
        }
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
        let response = self
            .apply(command)
            .await
            .unwrap_or(b"Entry Not Found".to_vec());
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(response))
            .unwrap())
    }

    pub(crate) async fn apply(&self, command: Command) -> Option<Vec<u8>> {
        let (tx, mut rx) = mpsc::channel(1);
        self.memtable_tx.send((command.clone(), tx)).await.unwrap();
        let entry = rx.recv().await.unwrap();
        if entry.is_some() {
            entry
        } else if let Command::Get { .. } = command {
            // If there is no entry for the key, search SSTables
            let (tx, mut rx) = mpsc::channel(1);
            self.sstable_tx.send((command, tx)).await.unwrap();
            rx.recv().await.unwrap()
        } else {
            None
        }
    }
}
