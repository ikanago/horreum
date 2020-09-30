mod server;

pub use server::Server;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Key and value not specified")]
    Empty,
    #[error("Key not specified")]
    LacksKey,
    #[error("Value not specified")]
    LacksValue,
    #[error("No entry for {0}")]
    NoEntry(String)
}
