use bincode::{deserialize, serialize, Error};
use std::io::{Cursor, Read};

/// Internal representation of a key-value pair.
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct InternalPair {
    pub(crate) key: Vec<u8>,
    /// If this pair is deleted, `value` is `None`.
    value: Option<Vec<u8>>,
}

impl InternalPair {
    /// Initialize `InternalPair`.
    pub fn new(key: &[u8], value: Option<&[u8]>) -> Self {
        Self {
            key: key.to_vec(),
            value: value.map(|v| v.to_vec()),
        }
    }

    /// Serialize struct's members into `Vec<u8>`.
    pub fn serialize(&self) -> Vec<u8> {
        let mut key_length = serialize(&self.key.len()).unwrap();
        let mut value_length = match &self.value {
            Some(value) => serialize(&value.len()).unwrap(),
            None => vec![0; 8],
        };
        let mut buffer = Vec::new();
        buffer.append(&mut key_length);
        buffer.append(&mut value_length);
        buffer.append(&mut self.key.clone());
        if let Some(value) = &self.value {
            buffer.append(&mut value.clone());
        }
        buffer
    }

    /// Serialize each elements in `pairs` and flatten vector of bytes.
    pub fn serialize_flatten(pairs: &[InternalPair]) -> Vec<u8> {
        pairs.iter().flat_map(|pair| pair.serialize()).collect()
    }

    /// Deserialize `Vec<u8>` into struct's members.
    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, Error> {
        InternalPair::deserialize_inner(reader)
    }

    /// Deserialize bytes of pairs.
    pub fn deserialize_from_bytes(bytes: &mut [u8]) -> Result<Vec<Self>, Error> {
        let mut pairs = vec![];
        let bytes_length = bytes.len() as u64;
        let mut cursor = Cursor::new(bytes);
        while cursor.position() < bytes_length {
            let pair = Self::deserialize_inner(&mut cursor)?;
            pairs.push(pair);
        }
        Ok(pairs)
    }

    // Deserialize key and value from something implemented `Read`
    // and return `Self` and the number of bytes read from.
    fn deserialize_inner<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut length_buffer = vec![0; 16];
        reader.read_exact(&mut length_buffer)?;
        let key_length: usize = deserialize(&length_buffer[..8])?;
        let value_length: usize = deserialize(&length_buffer[8..])?;
        let mut content_buffer = vec![0; key_length + value_length];
        reader.read_exact(&mut content_buffer)?;
        let key = content_buffer[..key_length].to_vec();
        let value = if value_length > 0 {
            Some(content_buffer[key_length..].to_vec())
        } else {
            None
        };
        Ok(InternalPair { key, value })
    }
}

impl Default for InternalPair {
    fn default() -> Self {
        Self::new(b"", None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize() {
        let pair = InternalPair::new("abc".as_bytes(), Some("defg".as_bytes()));
        assert_eq!(
            vec![3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 100, 101, 102, 103,],
            pair.serialize()
        );
    }

    #[test]
    fn serialize_lacking_value() {
        let pair = InternalPair::new("abc".as_bytes(), None);
        assert_eq!(
            vec![3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99],
            pair.serialize()
        );
    }

    #[test]
    fn serialize_non_ascii() {
        let pair = InternalPair::new("日本語💖".as_bytes(), Some("ржавчина".as_bytes()));
        assert_eq!(
            vec![
                13, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 230, 151, 165, 230, 156, 172,
                232, 170, 158, 240, 159, 146, 150, 209, 128, 208, 182, 208, 176, 208, 178, 209,
                135, 208, 184, 208, 189, 208, 176,
            ],
            pair.serialize()
        );
    }

    #[test]
    fn serialize_flatten() {
        let pairs = vec![
            InternalPair::new("abc00".as_bytes(), Some("def".as_bytes())),
            InternalPair::new("abc01".as_bytes(), Some("defg".as_bytes())),
            InternalPair::new("abc02".as_bytes(), Some("de".as_bytes())),
        ];
        assert_eq!(
            vec![
                5, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 48, 48, 100, 101, 102,
                5, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 48, 49, 100, 101, 102,
                103, 5, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 48, 50, 100, 101,
            ],
            InternalPair::serialize_flatten(&pairs)
        );
    }

    #[test]
    fn deserialize() {
        let bytes = vec![
            3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 100, 101, 102, 103,
        ];
        let pair = InternalPair::deserialize(&mut bytes.as_slice()).unwrap();
        assert_eq!(pair, InternalPair::new("abc".as_bytes(), Some("defg".as_bytes())));
    }

    #[test]
    fn deserialize_lacking_value() {
        let bytes = vec![3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99];
        let pair = InternalPair::deserialize(&mut bytes.as_slice()).unwrap();
        assert_eq!(InternalPair::new("abc".as_bytes(), None), pair);
    }

    #[test]
    fn deserialize_non_ascii() {
        let bytes = vec![
            13, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 230, 151, 165, 230, 156, 172, 232,
            170, 158, 240, 159, 146, 150, 209, 128, 208, 182, 208, 176, 208, 178, 209, 135, 208,
            184, 208, 189, 208, 176,
        ];
        let pair = InternalPair::deserialize(&mut bytes.as_slice()).unwrap();
        assert_eq!(InternalPair::new("日本語💖".as_bytes(), Some("ржавчина".as_bytes())), pair);
    }

    #[test]
    fn ordering() {
        assert!(
            InternalPair::new("abc".as_bytes(), Some("defg".as_bytes()))
                < InternalPair::new("日本語💖".as_bytes(), Some("ржавчина".as_bytes()))
        );
    }

    #[test]
    fn deserialize_from_bytes() {
        let pairs = vec![
            InternalPair::new("abc00".as_bytes(), Some("def".as_bytes())),
            InternalPair::new("abc01".as_bytes(), Some("defg".as_bytes())),
            InternalPair::new("abc02".as_bytes(), Some("de".as_bytes())),
            InternalPair::new("abc03".as_bytes(), Some("defgh".as_bytes())),
        ];
        let mut bytes: Vec<u8> = pairs.iter().flat_map(|pair| pair.serialize()).collect();
        assert_eq!(
            pairs,
            InternalPair::deserialize_from_bytes(&mut bytes).unwrap()
        );
    }
}
