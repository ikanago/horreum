use crate::http::QueryError;
use crate::index::Horreum;
use hyper::{service, Body, Method, Request, Response, StatusCode};
use log::{info, warn};
use qstring;
use std::io;
use std::net;
use std::sync::Arc;

pub struct Server {
    inner: hyper::Server<hyper::server::conn::AddrIncoming, ()>,
}

// impl Server {
//     pub fn new(port: u16, db: &Horreum) -> hyper::Server<hyper::server::conn::AddrIncoming, ()> {
//         let addr = net::IpAddr::from([127, 0, 0, 1]);
//         let addr = net::SocketAddr::new(addr, port);
//         hyper::Server::bind(&addr).serve(service::make_service_fn(move |_| async {
//             let db = db.clone();
//             Ok::<_, hyper::Error>(service::service_fn(move |req| async {
//                 handle(req, db);
//             }))
//         }))
//     }
// }

fn handle(request: Request<Body>, db: &Horreum) -> Result<Response<Body>, hyper::Error> {
    let response_message = match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => get(request.uri().query(), db),
        (&Method::POST, "/") => put(request.uri().query(), db),
        (&Method::DELETE, "/") => delete(request.uri().query(), db),
        _ => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap())
        }
    };
    match response_message {
        Ok(message) => Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(message))
            .unwrap()),
        Err(err) => Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(format!("{}", err)))
            .unwrap()),
    }
}

fn get(query: Option<&str>, db: &Horreum) -> Result<String, QueryError> {
    let key = get_key(query)?;
    let value = match db.get(&key) {
        Some(value) => value,
        None => return Ok(format!("No entry for {}", key)),
    };
    Ok(value)
}

fn put(query: Option<&str>, db: &Horreum) -> Result<String, QueryError> {
    let (key, value) = get_key_value(query)?;
    db.put(key, value);
    Ok("Put".to_string())
}

fn delete(query: Option<&str>, db: &Horreum) -> Result<String, QueryError> {
    let key = get_key(query)?;
    let deleted_value = match db.delete(&key) {
        Some(value) => value,
        None => return Ok(format!("No entry for {}", key)),
    };
    Ok(deleted_value)
}

/// Get key from a request URI.
/// It is used to delete or get data from an index.
fn get_key(query: Option<&str>) -> Result<String, QueryError> {
    let query = query.ok_or(QueryError::Empty)?;
    let query = qstring::QString::from(query);
    match query.get("key") {
        Some(key) => Ok(key.to_string()),
        None => Err(QueryError::LacksKey),
    }
}

/// Get key and value from a request URI.
/// They are used to put data into an index.
fn get_key_value(query: Option<&str>) -> Result<(String, String), QueryError> {
    let query = query.ok_or(QueryError::Empty)?;
    let query = qstring::QString::from(query);
    let key = query.get("key");
    let value = query.get("value");
    match (key, value) {
        (Some(key), Some(value)) => Ok((key.to_string(), value.to_string())),
        (None, Some(_)) => Err(QueryError::LacksKey),
        (Some(_), None) => Err(QueryError::LacksValue),
        _ => Err(QueryError::Empty),
    }
}

#[cfg(test)]
mod tests {
    use crate::http::server::*;

    #[test]
    #[should_panic]
    fn test_get_key_with_empty_query() {
        get_key(None).unwrap();
    }

    #[test]
    fn test_get_key() {
        let query = Some("key=abc");
        assert_eq!("abc".to_string(), get_key(query).unwrap());
    }

    #[test]
    #[should_panic]
    fn test_get_key_value_with_empty_query() {
        get_key_value(None).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_get_key_value_only_with_key() {
        let query = Some("key=abc");
        get_key_value(query).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_get_key_value_only_with_value() {
        let query = Some("value=def");
        get_key_value(query).unwrap();
    }

    #[test]
    fn test_get_key_value() {
        let query = Some("key=abc&value=def");
        assert_eq!(
            ("abc".to_string(), "def".to_string()),
            get_key_value(query).unwrap()
        );
    }
}
