use crate::http::QueryError;
use crate::index::Index;
use hyper::{service, Body, Method, Request, Response, Server, StatusCode};
use log::warn;
use std::convert::Infallible;
use std::net;

pub async fn serve(index: &Index, port: u16) -> Result<(), hyper::Error> {
    let addr = net::IpAddr::from([127, 0, 0, 1]);
    let addr = net::SocketAddr::new(addr, port);
    let service = service::make_service_fn(move |_| {
        let index = index.clone();
        async move {
            Ok::<_, Infallible>(service::service_fn(move |req| {
                let index = index.clone();
                async move { handle(req, &index).await }
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

async fn handle(request: Request<Body>, index: &Index) -> Result<Response<Body>, hyper::Error> {
    let response_message = match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => get(request.uri().query(), index),
        (&Method::POST, "/") => put(request.uri().query(), index),
        (&Method::DELETE, "/") => delete(request.uri().query(), index),
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
        Err(err) => {
            warn!("{}", err);
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("{}", err)))
                .unwrap())
        }
    }
}

fn get(query: Option<&str>, index: &Index) -> Result<String, QueryError> {
    let key = get_key(query)?;
    let value = match index.get(&key) {
        Some(value) => value,
        None => return Ok(format!("No entry for {}", key)),
    };
    Ok(value)
}

fn put(query: Option<&str>, index: &Index) -> Result<String, QueryError> {
    let (key, value) = get_key_value(query)?;
    index.put(key, value);
    Ok("Put".to_string())
}

fn delete(query: Option<&str>, index: &Index) -> Result<String, QueryError> {
    let key = get_key(query)?;
    let deleted_value = match index.delete(&key) {
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
