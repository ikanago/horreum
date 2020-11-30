use crate::format::InternalPair;

/// Block is a group of keys.
/// This has a first key of the block, position at a disk and length of the block.
#[derive(Debug, Eq, PartialEq)]
pub struct Block {
    /// First key of the block.
    key: Vec<u8>,
    /// Block's position at a disk.
    position: usize,
    /// Length of the block.
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
/// Holds array of `Block` which is a group of key-value pairs.  
/// Divide all pairs into `Block`s and every `Block` occupies at most `block_stride` pairs.
///
/// Example inner structure:
/// ```text
/// +===========+
/// |   Index   |     In a Disk
/// +===========+    +-----------------------------------------------------+
/// |  abc: 0   | -> | abc: aaa | abx: hoge | ascii: nyan | ... | dau: xxx | 1st Block(0 ~ 122 byte, 15 pairs)
/// +-----------+    +-----------------------------------------------------+
/// | data: 123 | -> | data: xxx | dd: hoge | euclid: fuga | ... | gg: hey | 2nd Block(123 = 269 byte, 15 pairs)
/// +-----------+    +-----------------------------------------------------+
/// | ghij: 270 | -> | ghij: fuxk | gcc: x | rust: pretty | ... | zzz: yes | 3rd Block(270 ~ 500 byte, 15 pairs)
/// +-----------+    +-----------------------------------------------------+
/// ```
#[derive(Debug)]
pub struct Index {
    items: Vec<Block>,
}

impl Index {
    /// Create index for key-value pairs stored in a disk.  
    /// Assume `pairs` is sorted.  
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

    #[test]
    fn index_creation() {
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
        let index = Index::new(pairs, 3);
        assert_eq!(None, index.get(b"a"));
        assert_eq!(Some((0, 72)), index.get(b"abc01"));
        assert_eq!(Some((72, 79)), index.get(b"abc03"));
        assert_eq!(Some((348, 21)), index.get(b"abc15"));
    }
}
