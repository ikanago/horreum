mod server;

pub use server::serve;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum QueryError {
    #[error("Key and value not specified")]
    Empty,
    #[error("Key not specified")]
    LacksKey,
    #[error("Value not specified")]
    LacksValue,
    #[error("Invalid HTTP method")]
    InvalidMethod,
}
