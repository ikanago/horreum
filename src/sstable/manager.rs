use super::storage::PersistedFile;
use super::table::SSTable;
use crate::command::Command;
use crate::format::InternalPair;
use crate::Message;
use log::{debug, info, warn};
use std::fs;
use std::io;
use std::mem;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

/// Manage multiple SSTable instances.
/// All operation to an SSTalbe is taken via this struct.
#[derive(Debug)]
pub struct SSTableManager {
    /// Directory to store SSTable's files.
    /// Files in the directory is sorted by thier name(like table_0, table_1, table_2...).
    /// File with bigger number at the end of the file name is newer one.
    table_directory: PathBuf,

    /// Every `block_stride` pair, `SSTable` creates an index entry.
    block_stride: usize,

    /// Array of SSTables this struct manages.
    /// Descending order by thier age (back elements is the newer).
    tables: Vec<SSTable>,

    /// Threshold to determine compaction should be acted.
    compaction_trigger_ratio: f64,

    /// Receiver to receive command.
    command_rx: mpsc::Receiver<Message>,
}

impl SSTableManager {
    /// Open existing SSTable files.
    pub async fn new<P: AsRef<Path>>(
        directory: P,
        block_stride: usize,
        compaction_trigger_ratio: u64,
        command_rx: mpsc::Receiver<Message>,
    ) -> io::Result<Self> {
        let mut table_directory = PathBuf::new();
        table_directory.push(directory);

        let mut paths: Vec<_> = fs::read_dir(&table_directory)?
            .into_iter()
            .filter_map(|path| path.ok())
            .collect();
        paths.sort_by_key(|path| path.path());
        let mut tables = Vec::new();
        for path in paths.iter() {
            tables.push(SSTable::open(path.path(), block_stride).await?)
        }
        let compaction_trigger_rate = compaction_trigger_ratio as f64 / 100.0;

        Ok(Self {
            table_directory,
            block_stride,
            tables,
            compaction_trigger_ratio: compaction_trigger_rate,
            command_rx,
        })
    }

    /// Create a new SSTable with given pairs.
    pub async fn create(&mut self, pairs: Vec<InternalPair>, size: usize) -> io::Result<()> {
        let table_path = self.new_table_path();
        let file = PersistedFile::new(table_path, &pairs).await?;
        let table = SSTable::new(file, pairs, size, self.block_stride)?;
        self.tables.push(table);
        Ok(())
    }

    /// Generate a path name for a new SSTable.
    fn new_table_path(&self) -> PathBuf {
        let mut table_path = self.table_directory.clone();
        table_path.push(format!("table_{}", self.tables.len()));
        table_path
    }

    /// Listen to channel to receive instruction to get data or create a new table with flushed
    /// data.
    pub async fn listen(&mut self) {
        loop {
            match self.command_rx.recv().await {
                Some((command, tx)) => match command {
                    Command::Get { key } => {
                        let entry = self
                            .get(&key)
                            .await
                            .unwrap()
                            .map(|pair| pair.value)
                            .flatten();
                        if tx.send(entry).is_err() {
                            warn!("The receiver already dropped");
                        }
                    }
                    // If `Command` does not include `Flush`
                    // * when this loop waits for an instruction to get a content or flush with
                    // async channel, contents in one of the two channel will never be received.
                    // * with sync channel, `Handler::apply()` does not wait for sending back
                    // result from here to receive it. This results in missing key-value pair which
                    // actually exists.
                    Command::Flush { pairs, size } => {
                        if let Err(err) = self.create(pairs, size).await {
                            warn!("{}", err);
                        }
                        if let Err(err) = self.compact().await {
                            warn!("{}", err);
                        }
                        // Just notify flush completion.
                        if tx.send(None).is_err() {
                            warn!("The receiver already dropped");
                        }
                    }
                    _ => (),
                },
                None => warn!("The channel disconnected"),
            }
        }
    }

    /// Get a pair by given key from SSTables.
    pub async fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        for table in self.tables.iter_mut().rev() {
            let pair = table.get(key).await?;
            if pair.is_some() {
                return Ok(pair);
            }
        }
        Ok(None)
    }

    /// Compact current all SSTables into a new one if a criteria is met.
    async fn compact(&mut self) -> io::Result<()> {
        let compacted_size = match self.should_compact() {
            Some(size) => size,
            None => {
                return Ok(());
            }
        };
        info!("Compactions has started");

        let mut tables = mem::replace(&mut self.tables, Vec::new());
        let mut table_iterators = Vec::new();
        for table in tables.iter_mut().rev() {
            let pairs = table.get_all().await?;
            table_iterators.push(pairs.into_iter());
        }
        let pairs = Self::compact_inner(table_iterators);

        for table in tables.iter_mut() {
            table.delete().await?;
        }
        self.create(pairs, compacted_size).await?;
        Ok(())
    }

    /// Determine compaction should be done.
    /// Implemented using following URL as a reference:
    /// https://github.com/facebook/rocksdb/wiki/Universal-Compaction#1-compaction-triggered-by-space-amplification
    /// Criteria:
    /// Let T1, T2, ..., Tn be SSTables where T1 is the newest one.
    /// Define `amplification_ratio` as (T1 + T2 + ... + Tn-1) / Tn.
    /// If `amplification_ratio` is greater than `self.compaction_trigger_rate`, compaction should
    /// be acted.
    /// This functions returns a size of compacted SSTable.
    fn should_compact(&self) -> Option<usize> {
        let oldest_table_size = match self.tables.first() {
            Some(table) => table.get_size(),
            None => {
                return None;
            }
        };
        let tables_total_size = self
            .tables
            .iter()
            .map(|table| table.get_size())
            .sum::<usize>();
        let newer_tables_total_size = tables_total_size - oldest_table_size;
        let amplification_ratio = newer_tables_total_size as f64 / oldest_table_size as f64;

        debug!(
            "amplification_ratio: {}, compaction_trigger_ratio: {}",
            amplification_ratio, self.compaction_trigger_ratio
        );
        if amplification_ratio > self.compaction_trigger_ratio {
            Some(tables_total_size)
        } else {
            None
        }
    }

    /// Read SSTable elements one by one for each SSTable and hold them as `merge_candidate`.
    /// Select a minimum key of them to keep sorted order.
    /// If there are multiple key of the same order, the newer one is selected.
    fn compact_inner(
        mut table_iterators: Vec<impl Iterator<Item = InternalPair>>,
    ) -> Vec<InternalPair> {
        // Array of current first elements for each SSTable.
        let mut merge_candidates = (0..table_iterators.len())
            .map(|i| table_iterators[i].next())
            .collect::<Vec<Option<InternalPair>>>();

        let mut pairs = Vec::new();
        loop {
            let min_pair = merge_candidates
                .iter()
                .filter(|pair| pair.is_some())
                .min_by_key(|pair| &pair.as_ref().unwrap().key)
                .unwrap()
                .as_ref()
                .unwrap()
                .clone();
            let min_key = min_pair.key.clone();
            pairs.push(min_pair);
            merge_candidates
                .iter_mut()
                .enumerate()
                .for_each(|(i, pair_opt)| {
                    if let Some(pair) = pair_opt {
                        if pair.key == min_key {
                            *pair_opt = table_iterators[i].next();
                        }
                    }
                });
            if merge_candidates.iter().all(|x| x.is_none()) {
                break;
            }
        }
        pairs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::*;
    use crate::PersistedContents;

    #[tokio::test]
    async fn open_existing_files() -> io::Result<()> {
        let path = "test_open_existing_files";
        std::fs::create_dir(path)?;
        let data0 = InternalPair::serialize_flatten(&vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"defg")),
        ]);
        let data1 = InternalPair::serialize_flatten(&vec![
            InternalPair::new(b"abc00", Some(b"xyz")),
            InternalPair::new(b"abc01", None),
        ]);
        let data2 =
            InternalPair::serialize_flatten(&vec![InternalPair::new(b"abc02", Some(b"def"))]);
        prepare_sstable_file("test_open_existing_files/table_0", &data0)?;
        prepare_sstable_file("test_open_existing_files/table_1", &data1)?;
        prepare_sstable_file("test_open_existing_files/table_2", &data2)?;

        let (_, crx) = mpsc::channel(4);
        let mut manager = SSTableManager::new(path, 2, 1000, crx).await?;
        assert_eq!(
            InternalPair::new(b"abc00", Some(b"xyz")),
            manager.get(b"abc00").await?.unwrap()
        );
        assert_eq!(
            InternalPair::new(b"abc01", None),
            manager.get(b"abc01").await?.unwrap()
        );
        assert_eq!(
            InternalPair::new(b"abc02", Some(b"def")),
            manager.get(b"abc02").await?.unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_pairs() -> io::Result<()> {
        let path = "test_get_create";
        std::fs::create_dir(path)?;
        let (_, crx) = mpsc::channel(4);
        let mut manager = SSTableManager::new(path, 2, 1000, crx).await?;
        manager
            .create(
                vec![
                    InternalPair::new(b"abc00", Some(b"def")),
                    InternalPair::new(b"abc01", Some(b"defg")),
                ],
                17,
            )
            .await?;
        manager
            .create(
                vec![
                    InternalPair::new(b"abc00", Some(b"xyz")),
                    InternalPair::new(b"abc01", None),
                ],
                13,
            )
            .await?;
        manager
            .create(vec![InternalPair::new(b"abc02", Some(b"def"))], 8)
            .await?;
        manager
            .create(vec![InternalPair::new(b"xxx", Some(b"42"))], 5)
            .await?;

        assert_eq!(
            InternalPair::new(b"abc00", Some(b"xyz")),
            manager.get(b"abc00").await?.unwrap()
        );
        assert_eq!(
            InternalPair::new(b"abc01", None),
            manager.get(b"abc01").await?.unwrap()
        );
        assert_eq!(
            InternalPair::new(b"abc02", Some(b"def")),
            manager.get(b"abc02").await?.unwrap()
        );
        assert_eq!(
            InternalPair::new(b"xxx", Some(b"42")),
            manager.get(b"xxx").await?.unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn compaction() -> io::Result<()> {
        let path = "test_compaction";
        std::fs::create_dir(path)?;
        let (_, crx) = mpsc::channel(4);
        let mut manager = SSTableManager::new(path, 2, 50, crx).await?;
        // Older, lower priority for reference
        manager
            .create(
                vec![
                    InternalPair::new(b"abc00", Some(b"def")),
                    InternalPair::new(b"abc01", Some(b"dog")),
                    InternalPair::new(b"abc02", None),
                    InternalPair::new(b"abc03", Some(b"cat")),
                ],
                29,
            )
            .await?;
        manager
            .create(
                vec![
                    InternalPair::new(b"abc00", Some(b"xyz")),
                    InternalPair::new(b"abc01", None),
                ],
                13,
            )
            .await?;
        // Newer, higher priority for reference
        manager
            .create(
                vec![
                    InternalPair::new(b"abc02", Some(b"fuga")),
                    InternalPair::new(b"abc04", Some(b"hoge")),
                ],
                18,
            )
            .await?;
        manager.compact().await?;

        let mut table = SSTable::open("test_compaction/table_0", 2).await?;
        assert_eq!(
            vec![
                InternalPair::new(b"abc00", Some(b"xyz")),
                InternalPair::new(b"abc01", None),
                InternalPair::new(b"abc02", Some(b"fuga")),
                InternalPair::new(b"abc03", Some(b"cat")),
                InternalPair::new(b"abc04", Some(b"hoge")),
            ],
            table.get_all().await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn should_act_compact() -> io::Result<()> {
        let path = "test_should_act_compact";
        let _ = std::fs::create_dir(path);
        let (_, crx) = mpsc::channel(4);
        let mut manager = SSTableManager::new(path, 2, 25, crx).await?;
        manager
            .create(vec![InternalPair::new(b"0123", None)], 4)
            .await?;
        manager
            .create(vec![InternalPair::new(b"0", None)], 1)
            .await?;
        manager
            .create(vec![InternalPair::new(b"0", None)], 1)
            .await?;
        // 1 1 4 => 6
        assert_eq!(Some(6), manager.should_compact());
        Ok(())
    }

    #[tokio::test]
    async fn should_not_act_compact() -> io::Result<()> {
        let path = "test_should_not_act_compact";
        let _ = std::fs::create_dir(path);
        let (_, crx) = mpsc::channel(4);
        let mut manager = SSTableManager::new(path, 2, 25, crx).await?;
        manager
            .create(vec![InternalPair::new(b"012345", None)], 6)
            .await?;
        manager
            .create(vec![InternalPair::new(b"0", None)], 1)
            .await?;
        // 1 6 => 1 6 (compaction not triggered)
        assert_eq!(None, manager.should_compact());
        Ok(())
    }
}
