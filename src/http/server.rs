use crate::index::Horreum;
use log::warn;
use qstring::QString;
use std::io;
use std::sync::Arc;
use tiny_http::{Method, Request, Response, Server};

pub fn listen(db: &Horreum) {
    let server = Arc::new(Server::http("127.0.0.1:8080").unwrap());
    let thread_num = 4;

    crossbeam::scope(|s| {
        for _ in 0..thread_num {
            let server = server.clone();
            s.spawn(move |_| {
                for request in server.incoming_requests() {
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
    dbg!(request.method(), request.url());
    if let Err(err) = request.respond(response) {
        warn!("{}", err);
    }
}

fn get(db: &Horreum, request: &Request) -> Response<io::Cursor<Vec<u8>>> {
    let key = match get_key(request.url()) {
        Some(key) => key,
        None => return tiny_http::Response::from_string("Specify key"),
    };
    let value = match db.get(&key) {
        Some(value) => value,
        None => return tiny_http::Response::from_string(format!("No entry for {}", key)),
    };
    tiny_http::Response::from_string(value)
}

fn put(db: &Horreum, request: &Request) -> Response<io::Cursor<Vec<u8>>> {
    let (key, value) = match get_key_value(request.url()) {
        Some((key, value)) => (key, value),
        None => return tiny_http::Response::from_string("Specify key and value"),
    };
    db.put(key, value);
    tiny_http::Response::from_string("Put")
}

fn delete(db: &Horreum, request: &Request) -> Response<io::Cursor<Vec<u8>>> {
    let key = match get_key(request.url()) {
        Some(key) => key,
        None => return tiny_http::Response::from_string("Specify key"),
    };
    let deleted_value = match db.delete(&key) {
        Some(value) => value,
        None => return tiny_http::Response::from_string(format!("No entry for {}", key)),
    };
    tiny_http::Response::from_string(deleted_value)
}

/// Get key from a request URI.
/// It is used to delete or get data from an index.
fn get_key(uri: &str) -> Option<String> {
    let query = match extract_query(uri) {
        Some(query) => query,
        None => return None,
    };
    query.get("key").map(|key| key.to_string())
}

/// Get key and value from a request URI.
/// They are used to put data into an index.
fn get_key_value(uri: &str) -> Option<(String, String)> {
    let query = match extract_query(uri) {
        Some(query) => query,
        None => return None,
    };
    let key = query.get("key").map(|key| key.to_string());
    let value = query.get("value").map(|key| key.to_string());
    match (key, value) {
        (Some(key), Some(value)) => Some((key, value)),
        _ => None,
    }
}

/// Extract query string from a request URI.
fn extract_query(uri: &str) -> Option<QString> {
    let query = uri.split('?').nth(1);
    let query = match query {
        Some(query) => query,
        None => return None,
    };
    Some(QString::from(query))
}

#[cfg(test)]
mod tests {
    use crate::http::server::*;

    #[test]
    fn test_get_key_with_empty_query() {
        let uri = "/";
        assert_eq!(None, get_key(uri));
    }

    #[test]
    fn test_get_key() {
        let uri = "/?key=abc";
        assert_eq!(Some("abc".to_string()), get_key(uri));
    }

    #[test]
    fn test_get_key_value_with_empty_query() {
        let uri = "/";
        assert_eq!(None, get_key_value(uri));
    }

    #[test]
    fn test_get_key_value_only_with_key() {
        let uri = "/?key=abc";
        assert_eq!(None, get_key_value(uri));
    }

    #[test]
    fn test_get_key_value_only_with_value() {
        let uri = "/?value=def";
        assert_eq!(None, get_key_value(uri));
    }

    #[test]
    fn test_get_key_value() {
        let uri = "/?key=abc&value=def";
        assert_eq!(
            Some(("abc".to_string(), "def".to_string())),
            get_key_value(uri)
        );
    }
}
