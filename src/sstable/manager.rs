use super::storage::PersistedFile;
use super::table::SSTable;
use crate::command::Command;
use crate::format::InternalPair;
use crate::Message;
use log::warn;
use std::cmp::Reverse;
use std::collections::VecDeque;
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
    /// Front element is the newer.
    tables: VecDeque<SSTable>,

    /// Receiver to receive command.
    command_rx: mpsc::Receiver<Message>,

    /// Receiver to receive flushed data.
    flushing_rx: mpsc::Receiver<Vec<InternalPair>>,
}

impl SSTableManager {
    /// Open existing SSTable files.
    pub async fn new<P: AsRef<Path>>(
        directory: P,
        block_stride: usize,
        command_rx: mpsc::Receiver<Message>,
        flushing_rx: mpsc::Receiver<Vec<InternalPair>>,
    ) -> io::Result<Self> {
        let mut table_directory = PathBuf::new();
        table_directory.push(directory);

        let mut paths: Vec<_> = fs::read_dir(&table_directory)?
            .into_iter()
            .filter_map(|path| path.ok())
            .collect();
        paths.sort_by_key(|path| Reverse(path.path()));
        let mut tables = VecDeque::new();
        for path in paths {
            tables.push_back(SSTable::open(path.path(), block_stride).await?)
        }
        Ok(Self {
            table_directory,
            block_stride,
            tables,
            command_rx,
            flushing_rx,
        })
    }

    /// Create a new SSTable with given pairs.
    pub async fn create(&mut self, pairs: Vec<InternalPair>) -> io::Result<()> {
        let table_path = self.new_table_path();
        let file = PersistedFile::new(table_path, &pairs).await?;
        let table = SSTable::new(file, pairs, self.block_stride).unwrap();
        self.tables.push_front(table);
        Ok(())
    }

    pub async fn listen(&mut self) {
        loop {
            if let Some((command, tx)) = self.command_rx.recv().await {
                if let Command::Get { key } = command {
                    let entry = self
                        .get(&key)
                        .await
                        .unwrap()
                        .map(|pair| pair.value)
                        .flatten();
                    if let Err(_) = tx.send(entry).await {
                        dbg!("The receiver dropped");
                    }
                }
            }
            if let Some(pairs) = self.flushing_rx.recv().await {
                dbg!("reached");
                if let Err(err) = self.create(pairs).await {
                    warn!("{}", err);
                }
            }
        }
    }

    /// Generate a path name for a new SSTable.
    fn new_table_path(&self) -> PathBuf {
        let mut table_path = self.table_directory.clone();
        table_path.push(format!("table_{}", self.tables.len()));
        table_path
    }

    /// Get a pair by given key from SSTables.
    pub async fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        for table in self.tables.iter_mut() {
            let pair = table.get(key).await?;
            if pair.is_some() {
                return Ok(pair);
            }
        }
        Ok(None)
    }

    /// Compact current all SSTables into a new one.
    pub async fn compact(&mut self) -> io::Result<()> {
        let tables = mem::replace(&mut self.tables, VecDeque::new());
        let mut table_iterators = Vec::new();
        for mut table in tables {
            let pairs = table.get_all().await?;
            table_iterators.push(pairs.into_iter());
        }
        let pairs = Self::compact_inner(table_iterators);

        let table_path = self.new_table_path();
        let file = PersistedFile::new(table_path, &pairs).await?;
        let merged_table = SSTable::new(file, pairs, self.block_stride)?;
        self.tables.push_front(merged_table);
        Ok(())
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

    #[tokio::test]
    async fn open_existing_files() -> io::Result<()> {
        let path = "test_open_existing_files";
        let _ = std::fs::create_dir(path);
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

        let (_, crx) = mpsc::channel(1);
        let (_, frx) = mpsc::channel(1);
        let mut manager = SSTableManager::new(path, 2, crx, frx).await?;
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
        let _ = std::fs::create_dir(path);
        let (_, crx) = mpsc::channel(1);
        let (_, frx) = mpsc::channel(1);
        let mut manager = SSTableManager::new(path, 2, crx, frx).await?;
        manager
            .create(vec![
                InternalPair::new(b"abc00", Some(b"def")),
                InternalPair::new(b"abc01", Some(b"defg")),
            ])
            .await?;
        manager
            .create(vec![
                InternalPair::new(b"abc00", Some(b"xyz")),
                InternalPair::new(b"abc01", None),
            ])
            .await?;
        manager
            .create(vec![InternalPair::new(b"abc02", Some(b"def"))])
            .await?;
        manager
            .create(vec![InternalPair::new(b"xxx", Some(b"42"))])
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

    #[test]
    fn compaction() {
        let tables = vec![
            // Higher priority for reference
            vec![
                InternalPair::new(b"abc02", Some(b"def")),
                InternalPair::new(b"abc04", Some(b"hoge")),
                InternalPair::new(b"abc05", None),
            ],
            vec![
                InternalPair::new(b"abc00", Some(b"xyz")),
                InternalPair::new(b"abc01", None),
            ],
            // lower priority for reference
            vec![
                InternalPair::new(b"abc00", Some(b"def")),
                InternalPair::new(b"abc01", Some(b"defg")),
                InternalPair::new(b"abc02", Some(b"xyz")),
                InternalPair::new(b"abc03", Some(b"defg")),
            ],
        ];
        let expected = vec![
            InternalPair::new(b"abc00", Some(b"xyz")),
            InternalPair::new(b"abc01", None),
            InternalPair::new(b"abc02", Some(b"def")),
            InternalPair::new(b"abc03", Some(b"defg")),
            InternalPair::new(b"abc04", Some(b"hoge")),
            InternalPair::new(b"abc05", None),
        ];
        let table_iterators = tables.into_iter().map(|table| table.into_iter()).collect();
        assert_eq!(expected, SSTableManager::compact_inner(table_iterators));
    }
}
