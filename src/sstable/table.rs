use crate::sstable::format::InternalPair;
use crate::sstable::index::{Block, Index};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Represents a SSTable.
#[derive(Debug)]
pub struct Table {
    /// Path to SSTable file
    path: PathBuf,
    /// Buffer of SSTable file.  
    /// Write action is taken just once, so it is not needed to use `BufWriter`.
    file_buffer: BufReader<File>,
    /// Stores pairs of key and position to start read the key from the file.
    pub(crate) index: Index,
}

impl Table {
    /// Create a new instance of `Table`.  
    /// Assume `pairs` is sorted.  
    /// Insert into an index every `block_stride` pair.
    pub fn new<P: AsRef<Path>>(
        path: P,
        pairs: Vec<InternalPair>,
        block_stride: usize,
    ) -> io::Result<Self> {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path_buf.clone().as_path())?;
        let mut index = Index::new();
        let mut read_data = Vec::new();

        for pair_chunk in pairs.chunks(block_stride) {
            let mut block = Block::new(&pair_chunk[0].key, read_data.len(), 0);
            let mut block_data: Vec<u8> = pair_chunk
                .iter()
                .flat_map(|pair| pair.serialize())
                .collect();
            block.set_length(block_data.len());
            index.push(block);
            read_data.append(&mut block_data);
        }
        file.write_all(&read_data)?;

        let file_buffer = BufReader::new(file);
        Ok(Self {
            path: path_buf,
            file_buffer,
            index,
        })
    }

    /// Get key-value pair from SSTable file.
    /// First, find block which stores the target pair.
    /// Then search the block from the front.
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        let (search_origin, length) = match self.index.get(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };
        self.file_buffer
            .seek(SeekFrom::Start(search_origin as u64))?;
        let mut block_bytes = vec![0; length];
        self.file_buffer.read(&mut block_bytes)?;

        // Handle this Result
        let pairs = InternalPair::deserialize_from_bytes(&block_bytes).unwrap();
        let pair = pairs.into_iter().find(|pair| pair.key == key);
        Ok(pair)
    }
}

impl std::ops::Drop for Table {
    /// Remove SSTable file when this is dropped.
    fn drop(&mut self) {
        std::fs::remove_file(self.path.as_path()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::read_file_to_buffer;

    #[test]
    fn table_creation() {
        let path = "test";
        let pairs = vec![
            InternalPair::new("abc", Some("defg")),
            InternalPair::new("abc", None),
            InternalPair::new("日本語💖", Some("ржавчина")),
        ];
        let expected: Vec<u8> = pairs.iter().flat_map(|pair| pair.serialize()).collect();
        let _table = Table::new(path, pairs, 1).unwrap();
        assert_eq!(expected, read_file_to_buffer(path));
    }

    #[test]
    fn search_table() {
        let path = "search_table";
        let pairs = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", Some("de")),
            InternalPair::new("abc03", Some("defgh")),
            InternalPair::new("abc04", Some("defg")),
            InternalPair::new("abc05", Some("defghij")),
            InternalPair::new("abc06", Some("def")),
            InternalPair::new("abc07", Some("defgh")),
            InternalPair::new("abc08", None),
            InternalPair::new("abc09", None),
            InternalPair::new("abc10", None),
            InternalPair::new("abc11", None),
            InternalPair::new("abc12", None),
            InternalPair::new("abc13", None),
            InternalPair::new("abc14", None),
            InternalPair::new("abc15", None),
        ];
        let mut table = Table::new(path, pairs, 3).unwrap();
        assert_eq!(
            InternalPair::new("abc04", Some("defg")),
            table.get("abc04".as_bytes()).unwrap().unwrap()
        );
        assert_eq!(
            InternalPair::new("abc15", None),
            table.get("abc15".as_bytes()).unwrap().unwrap()
        );
        assert_eq!(None, table.get("abc011".as_bytes()).unwrap());
        assert_eq!(None, table.get("abc16".as_bytes()).unwrap());
    }

    #[test]
    fn delete_table_file_in_drop() {
        let path = Path::new("delete_table_file");
        {
            let pairs = vec![
                InternalPair::new("abc00", Some("def")),
                InternalPair::new("abc01", Some("defg")),
                InternalPair::new("abc02", Some("de")),
            ];
            let _table = Table::new(path, pairs, 1);
            assert!(path.exists());
        }
        assert!(!path.exists());
    }
}
