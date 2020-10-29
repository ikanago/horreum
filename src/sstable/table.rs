use crate::sstable::format::InternalPair;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

/// Entries in SSTable's index.
/// This sturct holds bytes of a key and position of a disk which the key is stored.
#[derive(Debug)]
struct IndexEntries<'a> {
    items: Vec<(&'a [u8], u64)>,
}

impl<'a> IndexEntries<'a> {
    fn new() -> Self {
        Self { items: Vec::new() }
    }

    fn push(&mut self, key: &'a [u8], position: u64) {
        self.items.push((key, position));
    }

    /// Get a position of a key(`pair.key`) in a SSTable file.
    /// If the key does not exist in the index, return minimum position at which it should be.
    /// If the key is smaller than `self.items[0]` in dictionary order, return `None` because the key does not exist in the SSTable.
    #[allow(dead_code)]
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.items
            .binary_search_by_key(&key, |&(key, _)| key)
            .or_else(|pos| if pos > 0 { Ok(pos - 1) } else { Err(()) })
            .ok()
            .map(|pos| self.items[pos].1)
    }
}

/// Represents a SSTable.
#[derive(Debug)]
pub struct Table<'a> {
    // File to write data.
    file: File,
    // Stores pairs of key and position to start read the key from the file.
    index: IndexEntries<'a>,
}

impl<'a> Table<'a> {
    /// Create a new instance of `Table`.
    /// Assume `pairs` is sorted.
    /// Insert into an index every `index_stride` pair.
    pub fn new<P: AsRef<Path>>(
        path: P,
        pairs: Vec<InternalPair<'a>>,
        index_stride: usize,
    ) -> io::Result<Self> {
        let mut file = File::create(path)?;
        let mut index = IndexEntries::new();
        let mut read_bytes = 0;
        for pair_chunk in pairs.chunks(index_stride) {
            index.push(pair_chunk[0].key, read_bytes);
            for pair in pair_chunk {
                let bytes = pair.serialize();
                file.write_all(&bytes)?;
                read_bytes += bytes.len() as u64;
            }
        }
        Ok(Self { file, index })
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
                ([97, 98, 99, 48, 48].as_ref(), 0),
                (&[97, 98, 99, 48, 51], 75),
                (&[97, 98, 99, 48, 54], 157),
                (&[97, 98, 99, 48, 57], 223),
                (&[97, 98, 99, 49, 50], 265),
                (&[97, 98, 99, 49, 53], 307),
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
        assert_eq!(index.get("a".as_bytes()), None);
        assert_eq!(index.get("abc01".as_bytes()), Some(0));
        assert_eq!(index.get("abc03".as_bytes()), Some(75));
        assert_eq!(index.get("abc15".as_bytes()), Some(307));
    }
}
