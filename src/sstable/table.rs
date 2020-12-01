use super::index::Index;
use super::storage::PersistedFile;
use crate::format::InternalPair;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use tokio::io::AsyncReadExt;

/// Represents a SSTable.
#[derive(Debug)]
pub struct SSTable {
    /// API to access an SSTable file.
    pub(crate) file: PersistedFile,
    /// Stores pairs of key and position to start read the key from the file.
    pub(crate) index: Index,
}

impl SSTable {
    /// Create a new instance of `Table`.
    pub fn new(
        file: PersistedFile,
        pairs: Vec<InternalPair>,
        block_stride: usize,
    ) -> io::Result<Self> {
        let index = Index::new(pairs, block_stride);
        Ok(Self { file, index })
    }

    /// Open existing file and load key-value pairs in it.
    pub async fn open<P: AsRef<Path>>(path: P, block_stride: usize) -> io::Result<Self> {
        let mut file = PersistedFile::open(path).await?;
        let mut data = Vec::new();
        file.file.read_to_end(&mut data).await?;
        // Handle this Result
        let pairs = InternalPair::deserialize_from_bytes(&mut data).unwrap();
        let index = Index::new(pairs, block_stride);

        Ok(Self { file, index })
    }

    /// Get key-value pair from SSTable file.
    /// First, find block which stores the target pair.
    /// Then search the block from the front.
    pub async fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        let (search_origin, length) = match self.index.get(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };
        let mut block_bytes = self.file.read_at(search_origin, length).await?;

        // Handle this Result
        let pairs = InternalPair::deserialize_from_bytes(&mut block_bytes).unwrap();
        let pair = match pairs.binary_search_by_key(&key, |entry| &entry.key) {
            Ok(pos) => Some(pairs[pos].clone()),
            Err(_) => None,
        };
        Ok(pair)
    }
}

//impl IntoIterator for SSTable {
//    type Item = InternalPair;
//    type IntoIter = SSTableIterator;
//
//    fn into_iter(self) -> Self::IntoIter {
//        SSTableIterator::new(self.file)
//    }
//}
//
//#[derive(Debug)]
//pub struct SSTableIterator {
//    file: PersistedFile,
//}
//
//impl SSTableIterator {
//    pub fn new(file: PersistedFile) -> Self {
//        let mut file = file;
//        file.file.seek(SeekFrom::Start(0)).unwrap();
//        Self { file }
//    }
//}
//
//impl Iterator for SSTableIterator {
//    type Item = InternalPair;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        match InternalPair::deserialize(&mut self.file) {
//            Ok(pair) => {
//                if !pair.key.is_empty() {
//                    Some(pair)
//                } else {
//                    None
//                }
//            }
//            Err(_) => None,
//        }
//    }
//}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::*;

    #[tokio::test]
    async fn create_table() -> io::Result<()> {
        let path = "test_create_table";
        let pairs = vec![
            InternalPair::new(b"abc", Some(b"defg")),
            InternalPair::new(b"abc", None),
            InternalPair::new("日本語💖".as_bytes(), Some("ржавчина".as_bytes())),
        ];
        let file = PersistedFile::new(path, &pairs).await?;
        let _table = SSTable::new(file, pairs.clone(), 1)?;
        assert_eq!(
            InternalPair::serialize_flatten(&pairs),
            read_file_to_buffer(path)
        );
        Ok(())
    }

    #[tokio::test]
    async fn search_table() -> io::Result<()> {
        let path = "test_search_table";
        let pairs = vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"defg")),
            InternalPair::new(b"abc02", Some(b"de")),
            InternalPair::new(b"abc03", Some(b"defgh")),
            InternalPair::new(b"abc04", Some(b"defg")),
            InternalPair::new(b"abc05", Some(b"defghij")),
            InternalPair::new(b"abc06", Some(b"def")),
            InternalPair::new(b"abc07", Some(b"defgh")),
            InternalPair::new(b"abc08", None),
            InternalPair::new(b"abc09", None),
            InternalPair::new(b"abc10", None),
            InternalPair::new(b"abc11", None),
            InternalPair::new(b"abc12", None),
            InternalPair::new(b"abc13", None),
            InternalPair::new(b"abc14", None),
            InternalPair::new(b"abc15", None),
        ];
        let file = PersistedFile::new(path, &pairs).await?;
        let mut table = SSTable::new(file, pairs, 3)?;
        assert_eq!(
            Some(InternalPair::new(b"abc04", Some(b"defg"))),
            table.get(b"abc04").await?
        );
        assert_eq!(
            Some(InternalPair::new(b"abc15", None)),
            table.get(b"abc15").await?
        );
        assert_eq!(None, table.get(b"abc011").await?);
        assert_eq!(None, table.get(b"abc16").await?);
        Ok(())
    }

    /*
    #[tokio::test]
    async fn iterate_table() -> io::Result<()> {
        let path = "test_iterate_table";
        let pairs = vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"defg")),
            InternalPair::new(b"abc02", None),
        ];
        let file = PersistedFile::new(path, &pairs).await?;
        let table = SSTable::new(file, pairs, 3)?;
        let mut table_iter = table.into_iter();
        assert_eq!(
            Some(InternalPair::new(b"abc00", Some(b"def"))),
            table_iter.next()
        );
        assert_eq!(
            Some(InternalPair::new(b"abc01", Some(b"defg"))),
            table_iter.next()
        );
        assert_eq!(Some(InternalPair::new(b"abc02", None)), table_iter.next());
        assert_eq!(None, table_iter.next());
        Ok(())
    }

    #[tokio::test]
    async fn open_existing_file() -> io::Result<()> {
        let path = "test_open_existing_file";
        let pairs = vec![
            InternalPair::new(b"abc00", Some(b"def")),
            InternalPair::new(b"abc01", Some(b"defg")),
            InternalPair::new(b"abc02", None),
        ];
        let data = InternalPair::serialize_flatten(&pairs);
        prepare_sstable_file(path, &data)?;

        let table = SSTable::open(path, 3).await?;
        let opened_pairs: Vec<_> = table.into_iter().collect();
        assert_eq!(pairs, opened_pairs);
        Ok(())
    }
    */
}
