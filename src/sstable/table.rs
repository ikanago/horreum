use crate::sstable::format::InternalPair;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use crate::sstable::index::{Index, Block};

/// Represents a SSTable.
#[derive(Debug)]
pub struct Table {
    /// Buffer of SSTable's file.  
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
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
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
        Ok(Self { file_buffer, index })
    }

    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        let (search_origin, length) = match self.index.get(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };
        self.file_buffer
            .seek(SeekFrom::Start(search_origin as u64))?;
        let mut buffer = vec![0; length];
        self.file_buffer.read(&mut buffer)?;

        // Handle this Result
        let pairs = InternalPair::deserialize_from_bytes(&buffer).unwrap();
        let pair = pairs.into_iter().find(|pair| pair.key == key);
        Ok(pair)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::tests::{cleanup_file, read_file_to_buffer};
    
    #[test]
    fn table_creation() {
        let path = "test";
        let pairs = vec![
            InternalPair::new("abc", Some("defg")),
            InternalPair::new("abc", None),
            InternalPair::new("日本語💖", Some("ржавчина")),
        ];
        let expected: Vec<u8> = pairs.iter().flat_map(|pair| pair.serialize()).collect();
        let _ = Table::new(path, pairs, 1).unwrap();
        assert_eq!(expected, read_file_to_buffer(path));
        cleanup_file(path);
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
        cleanup_file(path);
    }
}