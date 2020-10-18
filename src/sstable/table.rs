use crate::sstable::format::InternalPair;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

#[derive(Debug)]
pub struct Table {
    file: File,
}

impl Table {
    pub fn new<P: AsRef<Path>>(path: P, pairs: Vec<InternalPair>) -> io::Result<Self> {
        let mut file = File::create(path)?;
        for pair in pairs {
            let bytes = pair.serialize();
            file.write(&bytes)?;
        }
        Ok(Self { file })
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

    #[test]
    fn table_creation() {
        let path = "test";
        let pairs = vec![
            InternalPair::new(("abc", Some("defg"))),
            InternalPair::new(("abc", None)),
            InternalPair::new(("日本語💖", Some("ржавчина"))),
        ];
        let expected: Vec<u8> = pairs.iter().flat_map(|pair| pair.serialize()).collect();
        let _ = Table::new(path, pairs).unwrap();
        assert_eq!(expected, read_file_to_buffer(path));
    }
}
