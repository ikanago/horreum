use bincode::{deserialize, serialize, Error};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};

/// Internal representation of a key-value pair.
#[derive(Clone, Debug, Deserialize, Serialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct InternalPair {
    pub(crate) key: Vec<u8>,
    /// If this pair is deleted, `value` is `None`.
    value: Option<Vec<u8>>,
}

impl InternalPair {
    /// Initialize `InternalPair`.
    /// # Example
    ///
    /// ```
    /// use horreum::sstable::format::InternalPair;
    ///
    /// let pair = InternalPair::new("abc", Some("def"));
    /// ```
    pub fn new(key: &str, value: Option<&str>) -> Self {
        Self {
            key: key.as_bytes().to_vec(),
            value: value.map(|v| v.as_bytes().to_vec()),
        }
    }

    /// Serialize struct's members into `Vec<u8>`.
    /// # Example
    ///
    /// ```
    /// use horreum::sstable::format::InternalPair;
    ///
    /// let pair = InternalPair::new("abc", Some("defg"));
    /// assert_eq!(
    ///     vec![
    ///         3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 100, 101, 102, 103,
    ///     ],
    ///     pair.serialize()
    /// );
    /// ```
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
    ///
    /// # Example
    ///
    /// ```
    /// use horreum::sstable::format::InternalPair;
    /// let pairs = vec![
    ///     InternalPair::new("abc00", Some("def")),
    ///     InternalPair::new("abc01", Some("defg")),
    ///     InternalPair::new("abc02", Some("de")),
    /// ];
    /// assert_eq!(vec![
    ///        5, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 48, 48, 100, 101, 102,
    ///        5, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 48, 49, 100, 101, 102, 103,
    ///        5, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 48, 50, 100, 101,
    ///     ],
    ///     InternalPair::serialize_flatten(&pairs)
    /// );
    /// ```
    pub fn serialize_flatten(pairs: &[InternalPair]) -> Vec<u8> {
        pairs.iter().flat_map(|pair| pair.serialize()).collect()
    }

    /// Deserialize `Vec<u8>` into struct's members.
    /// # Example
    ///
    /// ```
    /// use horreum::sstable::format::InternalPair;
    ///
    /// let bytes = vec![
    ///     3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 100, 101, 102, 103,
    /// ];
    /// let pair = InternalPair::deserialize(&mut bytes.as_slice()).unwrap();
    /// assert_eq!(
    ///     pair,
    ///     InternalPair::new("abc", Some("defg"))
    /// );
    /// ```
    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, Error> {
        InternalPair::deserialize_inner(reader)
    }

    /// Deserialize bytes of pairs.
    /// # Example
    ///
    /// ```
    /// use horreum::sstable::format::InternalPair;
    /// let pairs = vec![
    ///     InternalPair::new("abc00", Some("def")),
    ///     InternalPair::new("abc01", Some("defg")),
    ///     InternalPair::new("abc02", Some("de")),
    ///     InternalPair::new("abc03", Some("defgh")),
    /// ];
    /// let mut bytes: Vec<u8> = pairs.iter().flat_map(|pair| pair.serialize()).collect();
    /// assert_eq!(pairs, InternalPair::deserialize_from_bytes(&mut bytes).unwrap());
    /// ```
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
        Self::new("", None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_lacking_value() {
        let pair = InternalPair::new("abc", None);
        assert_eq!(
            vec![3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99],
            pair.serialize()
        );
    }

    #[test]
    fn serialize_non_ascii() {
        let pair = InternalPair::new("日本語💖", Some("ржавчина"));
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
    fn deserialize_lacking_value() {
        let bytes = vec![3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99];
        let pair = InternalPair::deserialize(&mut bytes.as_slice()).unwrap();
        assert_eq!(InternalPair::new("abc", None), pair);
    }

    #[test]
    fn deserialize_non_ascii() {
        let bytes = vec![
            13, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 230, 151, 165, 230, 156, 172, 232,
            170, 158, 240, 159, 146, 150, 209, 128, 208, 182, 208, 176, 208, 178, 209, 135, 208,
            184, 208, 189, 208, 176,
        ];
        let pair = InternalPair::deserialize(&mut bytes.as_slice()).unwrap();
        assert_eq!(InternalPair::new("日本語💖", Some("ржавчина")), pair);
    }

    #[test]
    fn ordering() {
        assert!(
            InternalPair::new("abc", Some("defg"))
                < InternalPair::new("日本語💖", Some("ржавчина"))
        );
    }

    #[test]
    fn deserialize_from_bytes() {
        let pairs = vec![
            InternalPair::new("abc00", Some("def")),
            InternalPair::new("abc01", Some("defg")),
            InternalPair::new("abc02", Some("de")),
            InternalPair::new("abc03", Some("defgh")),
        ];
        let mut bytes: Vec<u8> = pairs.iter().flat_map(|pair| pair.serialize()).collect();
        assert_eq!(
            pairs,
            InternalPair::deserialize_from_bytes(&mut bytes).unwrap()
        );
    }
}
