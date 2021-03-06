mod command;
mod config;
mod error;
mod format;
pub mod http;
pub mod memtable;
pub mod sstable;

pub use crate::config::Config;
pub use crate::http::server::serve;
pub use memtable::MemTable;
pub use sstable::manager::SSTableManager;

use command::Command;
use tokio::sync::oneshot;

/// Message sent to a store(`MemTable` or `SSTableManager`).
/// This holds `mpsc::Sender` because the store have to send back response
/// to sender of the `Message`.
type Message = (Command, oneshot::Sender<Option<Vec<u8>>>);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::InternalPair;
    use crate::http::server::Handler;
    use std::io;
    use tokio::sync::mpsc;

    const MEMTABLE_SIZE: usize = 128;

    #[tokio::test]
    async fn put_and_get_integrated() -> io::Result<()> {
        let (memtable_tx, memtable_rx) = mpsc::channel(1);
        let (sstable_tx, sstable_rx) = mpsc::channel(32);
        let mut memtable = MemTable::new(MEMTABLE_SIZE, memtable_rx, sstable_tx.clone());

        let directory = "test_put_and_get";
        let _ = std::fs::create_dir(directory);
        let mut manager = SSTableManager::new(directory, 3, 1000, sstable_rx).await?;
        manager
            .create(
                vec![
                    InternalPair::new(b"rust", Some(b"wonderful")),
                    InternalPair::new(b"xxx", Some(b"sstable")),
                ],
                23,
            )
            .await?;

        tokio::spawn(async move { memtable.listen().await });
        tokio::spawn(async move { manager.listen().await });

        let handler = Handler::new(memtable_tx, sstable_tx);
        handler
            .apply(Command::Put {
                key: b"abc".to_vec(),
                value: b"def".to_vec(),
            })
            .await;
        handler
            .apply(Command::Put {
                key: b"xxx".to_vec(),
                value: b"memtable".to_vec(),
            })
            .await;

        // Simply read from MemTable
        assert_eq!(
            b"def".to_vec(),
            handler
                .apply(Command::Get {
                    key: b"abc".to_vec()
                })
                .await
                .unwrap()
        );
        // Exists the same entry in SSTable, but read from MemTable
        assert_eq!(
            b"memtable".to_vec(),
            handler
                .apply(Command::Get {
                    key: b"xxx".to_vec()
                })
                .await
                .unwrap()
        );
        // Simply read from SSTable
        assert_eq!(
            b"wonderful".to_vec(),
            handler
                .apply(Command::Get {
                    key: b"rust".to_vec(),
                })
                .await
                .unwrap()
        );
        Ok(())
    }
}
