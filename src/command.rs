use crate::error::Error;
use crate::format::InternalPair;
use hyper::Method;
use qstring::QString;

/// Represents actions to key-value store and holds necessary data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Flush { pairs: Vec<InternalPair> },
}

impl Command {
    pub fn new(method: &Method, query: Option<&str>) -> Result<Command, Error> {
        match method {
            &Method::GET => Ok(Command::Get {
                key: get_key(query)?,
            }),
            &Method::PUT => {
                let (key, value) = get_key_value(query)?;
                Ok(Command::Put { key, value })
            }
            &Method::DELETE => Ok(Command::Delete {
                key: get_key(query)?,
            }),
            _ => Err(Error::InvalidMethod),
        }
    }
}

/// Get key from a request URI.
fn get_key(query: Option<&str>) -> Result<Vec<u8>, Error> {
    let query = query.ok_or(Error::EmptyQuery)?;
    let query = QString::from(query);
    match query.get("key") {
        Some(key) => Ok(key.as_bytes().to_vec()),
        None => Err(Error::LacksKey),
    }
}

/// Get key and value from a request URI.
fn get_key_value(query: Option<&str>) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let query = query.ok_or(Error::EmptyQuery)?;
    let query = QString::from(query);
    let key = query.get("key");
    let value = query.get("value");
    match (key, value) {
        (Some(key), Some(value)) => Ok((key.as_bytes().to_vec(), value.as_bytes().to_vec())),
        (None, Some(_)) => Err(Error::LacksKey),
        (Some(_), None) => Err(Error::LacksValue),
        _ => Err(Error::EmptyQuery),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_get() {
        assert_eq!(
            Command::Get {
                key: b"abc".to_vec(),
            },
            Command::new(&Method::GET, Some("key=abc")).unwrap()
        );
    }

    #[test]
    fn command_put() {
        assert_eq!(
            Command::Put {
                key: b"abc".to_vec(),
                value: b"def".to_vec(),
            },
            Command::new(&Method::PUT, Some("key=abc&value=def")).unwrap()
        );
    }

    #[test]
    fn command_delete() {
        assert_eq!(
            Command::Delete {
                key: b"abc".to_vec(),
            },
            Command::new(&Method::DELETE, Some("key=abc")).unwrap()
        );
    }

    #[test]
    fn invalid_method() {
        assert_eq!(
            Err(Error::InvalidMethod),
            Command::new(&Method::POST, Some("key=a&value=b"))
        );
    }

    #[test]
    #[should_panic]
    fn test_get_key_with_empty_query() {
        get_key(None).unwrap();
    }

    #[test]
    fn test_get_key() {
        let query = Some("key=abc");
        assert_eq!(b"abc".to_vec(), get_key(query).unwrap());
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
            (b"abc".to_vec(), b"def".to_vec()),
            get_key_value(query).unwrap()
        );
    }
}
