/// Block is a group of keys.
/// This has a first key, position at a disk and length of the keys.
#[derive(Debug, Eq, PartialEq)]
pub struct Block {
    key: Vec<u8>,
    position: usize,
    length: usize,
}

impl Block {
    pub fn new(key: &[u8], position: usize, length: usize) -> Self {
        Self {
            key: key.to_vec(),
            position,
            length,
        }
    }

    pub fn set_length(&mut self, length: usize) {
        self.length = length;
    }
}

/// Entries in SSTable's index.
/// This sturct holds bytes of a key, position of a disk which the key is stored and length of the keys.
#[derive(Debug)]
pub struct Index {
    items: Vec<Block>,
}

impl Index {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn push(&mut self, block: Block) {
        self.items.push(block);
    }

    /// Get a position of a key(`pair.key`) in a SSTable file.
    /// If the key does not exist in the index, return minimum position at which it should be.
    /// If the key is smaller than `self.items[0]` in dictionary order, return `None` because the key does not exist in the SSTable.
    #[allow(dead_code)]
    pub fn get(&self, key: &[u8]) -> Option<(usize, usize)> {
        self.items
            .binary_search_by_key(&key, move |entry| &entry.key)
            .or_else(|pos| if pos > 0 { Ok(pos - 1) } else { Err(()) })
            .ok()
            .map(|pos| (self.items[pos].position, self.items[pos].length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::format::InternalPair;
    use crate::sstable::table::SSTable;
    use crate::sstable::tests::*;

    #[test]
    fn index_creation() {
        let path = "index_test";
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
        let table = SSTable::new(path, pairs, 3).unwrap();
        assert_eq!(
            vec![
                Block::new(&[97, 98, 99, 48, 48], 0, 72),
                Block::new(&[97, 98, 99, 48, 51], 72, 79),
                Block::new(&[97, 98, 99, 48, 54], 151, 71),
                Block::new(&[97, 98, 99, 48, 57], 222, 63),
                Block::new(&[97, 98, 99, 49, 50], 285, 63),
                Block::new(&[97, 98, 99, 49, 53], 348, 21),
            ],
            table.index.items
        );
        remove_sstable_file(path);
    }

    #[test]
    fn index_get() {
        let path = "index_get";
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
        let table = SSTable::new(path, pairs, 3).unwrap();
        assert_eq!(None, table.index.get("a".as_bytes()));
        assert_eq!(Some((0, 72)), table.index.get("abc01".as_bytes()));
        assert_eq!(Some((72, 79)), table.index.get("abc03".as_bytes()));
        assert_eq!(Some((348, 21)), table.index.get("abc15".as_bytes()));
        remove_sstable_file(path);
    }
}
