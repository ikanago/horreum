use crate::sstable::format::InternalPair;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Block is a group of keys.
/// This has a first key, position at a disk and length of the keys.
#[derive(Debug, Eq, PartialEq)]
struct Block {
    key: Vec<u8>,
    position: usize,
    length: usize,
}

impl Block {
    fn new(key: &[u8], position: usize, length: usize) -> Self {
        Self {
            key: key.to_vec(),
            position,
            length,
        }
    }

    fn set_length(&mut self, length: usize) {
        self.length = length;
    }
}

/// Entries in SSTable's index.
/// This sturct holds bytes of a key, position of a disk which the key is stored and length of the keys.
#[derive(Debug)]
struct Index {
    items: Vec<Block>,
}

impl Index {
    fn new() -> Self {
        Self { items: Vec::new() }
    }

    fn push(&mut self, block: Block) {
        self.items.push(block);
    }

    /// Get a position of a key(`pair.key`) in a SSTable file.
    /// If the key does not exist in the index, return minimum position at which it should be.
    /// If the key is smaller than `self.items[0]` in dictionary order, return `None` because the key does not exist in the SSTable.
    #[allow(dead_code)]
    fn get(&self, key: &[u8]) -> Option<(usize, usize)> {
        self.items
            .binary_search_by_key(&key, move |entry| &entry.key)
            .or_else(|pos| if pos > 0 { Ok(pos - 1) } else { Err(()) })
            .ok()
            .map(|pos| (self.items[pos].position, self.items[pos].length))
    }
}

/// Represents a SSTable.
#[derive(Debug)]
pub struct Table {
    // File to write data.
    file: File,
    // Stores pairs of key and position to start read the key from the file.
    index: Index,
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
        Ok(Self { file, index })
    }

    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<InternalPair>> {
        let (search_origin, length) = match self.index.get(key) {
            Some(pos) => pos,
            None => return Ok(None),
        };
        self.file.seek(SeekFrom::Start(search_origin as u64))?;
        let mut buffer = vec![0; length];
        self.file.read(&mut buffer)?;

        // Handle this Result
        let pairs = InternalPair::deserialize_from_bytes(buffer).unwrap();
        let pair = pairs.into_iter().find(|pair| pair.key == key);
        Ok(pair)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Read;

    fn read_file_to_buffer<P: AsRef<Path>>(path: P) -> Vec<u8> {
        let mut file = File::open(path).unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();
        buffer
    }

    fn cleanup_file<P: AsRef<Path>>(path: P) {
        std::fs::remove_file(path).unwrap()
    }

    #[test]
    fn table_creation() {
        let path = "test";
        let pairs = vec![
            InternalPair::new(("abc", Some("defg"))),
            InternalPair::new(("abc", None)),
            InternalPair::new(("日本語💖", Some("ржавчина"))),
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
            InternalPair::new(("abc00", Some("def"))),
            InternalPair::new(("abc01", Some("defg"))),
            InternalPair::new(("abc02", Some("de"))),
            InternalPair::new(("abc03", Some("defgh"))),
            InternalPair::new(("abc04", Some("defg"))),
            InternalPair::new(("abc05", Some("defghij"))),
            InternalPair::new(("abc06", Some("def"))),
            InternalPair::new(("abc07", Some("defgh"))),
            InternalPair::new(("abc08", None)),
            InternalPair::new(("abc09", None)),
            InternalPair::new(("abc10", None)),
            InternalPair::new(("abc11", None)),
            InternalPair::new(("abc12", None)),
            InternalPair::new(("abc13", None)),
            InternalPair::new(("abc14", None)),
            InternalPair::new(("abc15", None)),
        ];
        let mut table = Table::new(path, pairs, 3).unwrap();
        assert_eq!(
            InternalPair::new(("abc04", Some("defg"))),
            table.get("abc04".as_bytes()).unwrap().unwrap()
        );
        assert_eq!(
            InternalPair::new(("abc15", None)),
            table.get("abc15".as_bytes()).unwrap().unwrap()
        );
        assert_eq!(None, table.get("abc011".as_bytes()).unwrap());
        assert_eq!(None, table.get("abc16".as_bytes()).unwrap());
        cleanup_file(path);
    }

    #[test]
    fn index_creation() {
        let path = "index_test";
        let pairs = vec![
            InternalPair::new(("abc00", Some("def"))),
            InternalPair::new(("abc01", Some("defg"))),
            InternalPair::new(("abc02", Some("de"))),
            InternalPair::new(("abc03", Some("defgh"))),
            InternalPair::new(("abc04", Some("defg"))),
            InternalPair::new(("abc05", Some("defghij"))),
            InternalPair::new(("abc06", Some("def"))),
            InternalPair::new(("abc07", Some("defgh"))),
            InternalPair::new(("abc08", None)),
            InternalPair::new(("abc09", None)),
            InternalPair::new(("abc10", None)),
            InternalPair::new(("abc11", None)),
            InternalPair::new(("abc12", None)),
            InternalPair::new(("abc13", None)),
            InternalPair::new(("abc14", None)),
            InternalPair::new(("abc15", None)),
        ];
        let table = Table::new(path, pairs, 3).unwrap();
        cleanup_file(path);
        assert_eq!(
            vec![
                Block::new(&[97, 98, 99, 48, 48], 0, 75),
                Block::new(&[97, 98, 99, 48, 51], 75, 82),
                Block::new(&[97, 98, 99, 48, 54], 157, 66),
                Block::new(&[97, 98, 99, 48, 57], 223, 42),
                Block::new(&[97, 98, 99, 49, 50], 265, 42),
                Block::new(&[97, 98, 99, 49, 53], 307, 14),
            ],
            table.index.items
        );
    }

    #[test]
    fn index_get() {
        let path = "index_get";
        let pairs = vec![
            InternalPair::new(("abc00", Some("def"))),
            InternalPair::new(("abc01", Some("defg"))),
            InternalPair::new(("abc02", Some("de"))),
            InternalPair::new(("abc03", Some("defgh"))),
            InternalPair::new(("abc04", Some("defg"))),
            InternalPair::new(("abc05", Some("defghij"))),
            InternalPair::new(("abc06", Some("def"))),
            InternalPair::new(("abc07", Some("defgh"))),
            InternalPair::new(("abc08", None)),
            InternalPair::new(("abc09", None)),
            InternalPair::new(("abc10", None)),
            InternalPair::new(("abc11", None)),
            InternalPair::new(("abc12", None)),
            InternalPair::new(("abc13", None)),
            InternalPair::new(("abc14", None)),
            InternalPair::new(("abc15", None)),
        ];
        let table = Table::new(path, pairs, 3).unwrap();
        cleanup_file(path);
        let index = table.index;
        dbg!(&index);
        assert_eq!(None, index.get("a".as_bytes()));
        assert_eq!(Some((0, 75)), index.get("abc01".as_bytes()));
        assert_eq!(Some((75, 82)), index.get("abc03".as_bytes()));
        assert_eq!(Some((307, 14)), index.get("abc15".as_bytes()));
    }
}
