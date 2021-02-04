use crate::command::Command;
use crate::Message;
use hyper::server::Server;
use hyper::{service, Body, Request, Response, StatusCode};
use log::{debug, info, warn};
use std::convert::Infallible;
use std::net;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Start running server.
/// Clone handler for each request and spawn job for it.
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
                debug!("{:?}", &req);
                let handler = handler.clone();
                async move { handler.handle(req).await }
            }))
        }
    });

    info!("Server has started running at port {}", port);
    if let Err(e) = Server::bind(&addr).serve(service).await {
        warn!("{}", e);
        return Err(e);
    }
    Ok(())
}

/// Structure to handle command and communicate with `MemTable` and `SSTableManager`.
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

    /// Apply a command parsed from request to the stores.
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

    /// Communicate with the stores to apply a command
    pub(crate) async fn apply(&self, command: Command) -> Option<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.memtable_tx.send((command.clone(), tx)).await.unwrap();
        let entry = rx.await.unwrap();
        if entry.is_some() {
            entry
        } else if let Command::Get { .. } = command {
            // If there is no entry for the key, search SSTables
            let (tx, rx) = oneshot::channel();
            if let Err(_) = self.sstable_tx.send((command, tx)).await {
                warn!("The receiver dropped");
            }
            rx.await.unwrap()
        } else {
            None
        }
    }
}
