use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("Key and value not specified")]
    EmptyQuery,

    #[error("Key not specified")]
    LacksKey,

    #[error("Value not specified")]
    LacksValue,

    #[error("Invalid HTTP method")]
    InvalidMethod,
}
