use crate::sstable::format::InternalPair;
/// Block is a group of keys.
/// This has a first key of the block, position at a disk and length of the block.
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
/// This sturct holds array of `Block`.
/// `Block` is a group of key-value pairs.
///
/// ```text
/// +===========+
/// |   Index   |     In a Disk
/// +===========+    +-----------------------------------------------------+
/// |  abc: 0   | -> | abc: aaa | abx: hoge | ascii: nyan | ... | dau: xxx | 1st Block(0 ~ 122 byte)
/// +-----------+    +-----------------------------------------------------+
/// | data: 123 | -> | data: xxx | dd: hoge | euclid: fuga | ... | gg: hey | 2nd Block(123 = 269 byte)
/// +-----------+    +-----------------------------------------------------+
/// | ghij: 270 | -> | ghij: fuxk | gcc: x | rust: pretty | ... | zzz: yes | 3rd Block(270 ~ 500 byte)
/// +-----------+    +-----------------------------------------------------+
/// ```
#[derive(Debug)]
pub struct Index {
    items: Vec<Block>,
}

impl Index {
    /// Create index for key-value pairs stored in a disk.  
    /// Assume `pairs` is sorted.  
    /// Insert into an index every `block_stride` pair.
    pub fn new(pairs: Vec<InternalPair>, block_stride: usize) -> Self {
        let mut items = Vec::new();
        let mut read_data = Vec::new();

        for pair_chunk in pairs.chunks(block_stride) {
            let mut block = Block::new(&pair_chunk[0].key, read_data.len(), 0);
            let mut block_data = InternalPair::serialize_flatten(pair_chunk);
            block.set_length(block_data.len());
            items.push(block);
            read_data.append(&mut block_data);
        }
        Self { items }
    }

    /// Get a position of a key(`pair.key`) in a SSTable file.
    /// If the key does not exist in the index, return minimum position at which it should be.
    /// If the key is smaller than `self.items[0]` in dictionary order, return `None` because the key does not exist in the SSTable.
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

    #[test]
    fn index_creation() {
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
        let index = Index::new(pairs, 3);
        assert_eq!(
            vec![
                Block::new(&[97, 98, 99, 48, 48], 0, 72),
                Block::new(&[97, 98, 99, 48, 51], 72, 79),
                Block::new(&[97, 98, 99, 48, 54], 151, 71),
                Block::new(&[97, 98, 99, 48, 57], 222, 63),
                Block::new(&[97, 98, 99, 49, 50], 285, 63),
                Block::new(&[97, 98, 99, 49, 53], 348, 21),
            ],
            index.items
        );
    }

    #[test]
    fn index_get() {
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
        let index = Index::new(pairs, 3);
        assert_eq!(None, index.get("a".as_bytes()));
        assert_eq!(Some((0, 72)), index.get("abc01".as_bytes()));
        assert_eq!(Some((72, 79)), index.get("abc03".as_bytes()));
        assert_eq!(Some((348, 21)), index.get("abc15".as_bytes()));
    }
}
