mod format;
pub mod horreum;
pub mod http;
pub mod index;
mod memtable;
mod sstable;

pub use crate::horreum::Horreum;
pub use http::serve;

pub(crate) enum Command<'a> {
    Get { key: &'a [u8] },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: &'a [u8] },
}
