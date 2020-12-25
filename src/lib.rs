mod command;
mod error;
mod format;
pub mod http;
pub mod memtable;
pub mod sstable;

pub use crate::http::serve;
pub use memtable::MemTable;

use command::Command;
use memtable::Entry;
use tokio::sync::mpsc::Sender;

/// Message sent to a store(`MemTable` or `SSTableManager`).
/// This holds `oneshot::Sender` because the store have to send back response
/// to sender of the `Message`.
type Message = (Command, Sender<Option<Entry>>);
