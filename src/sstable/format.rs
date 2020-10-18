use bincode::{deserialize, serialize, Error};
use serde::{Deserialize, Serialize};

/// Internal representation of a key-value pair.
#[derive(Clone, Debug, Deserialize, Serialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct InternalPair<'a> {
    key: &'a [u8],
    /// If this pair is deleted, `value` is `None`.
    value: Option<&'a [u8]>,
}

impl<'a> InternalPair<'a> {
    pub fn new(pair: (&'a str, Option<&'a str>)) -> Self {
        Self {
            key: pair.0.as_bytes(),
            value: pair.1.map(|v| v.as_bytes()),
        }
    }

    /// Serialize struct's members into `Vec<u8>`.
    /// # Examples
    ///
    /// ```
    /// use horreum::sstable::format::InternalPair;
    ///
    /// let data = ("abc", Some("defg"));
    /// let pair = InternalPair::new(data);
    /// assert_eq!(
    ///     vec![
    ///         3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 1, 4, 0, 0, 0, 0, 0, 0, 0, 100, 101, 102, 103,
    ///     ],
    ///     pair.serialize()
    /// );
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        serialize(self).unwrap()
    }

    /// Deserialize struct's members from `Vec<u8>`
    /// # Examples
    ///
    /// ```
    /// use horreum::sstable::format::InternalPair;
    ///
    /// let bytes = vec![
    ///     3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 1, 4, 0, 0, 0, 0, 0, 0, 0, 100, 101, 102, 103,
    /// ];
    /// let pair = InternalPair::deserialize(&bytes).unwrap();
    /// assert_eq!(
    ///     pair,
    ///     InternalPair::new(("abc", Some("defg")))
    /// );
    /// ```
    pub fn deserialize(bytes: &'a [u8]) -> Result<Self, Error> {
        deserialize(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_lacking_value() {
        let pair = InternalPair::new(("abc", None));
        assert_eq!(
            vec![3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 0,],
            pair.serialize()
        );
    }

    #[test]
    fn serialize_non_ascii() {
        let pair = InternalPair::new(("日本語💖", Some("ржавчина")));
        assert_eq!(
            vec![
                13, 0, 0, 0, 0, 0, 0, 0, 230, 151, 165, 230, 156, 172, 232, 170, 158, 240, 159,
                146, 150, 1, 16, 0, 0, 0, 0, 0, 0, 0, 209, 128, 208, 182, 208, 176, 208, 178, 209,
                135, 208, 184, 208, 189, 208, 176,
            ],
            pair.serialize()
        );
    }

    #[test]
    fn deserialize_lacking_value() {
        let bytes = vec![3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 0];
        let pair = InternalPair::deserialize(&bytes).unwrap();
        assert_eq!(InternalPair::new(("abc", None)), pair);
    }

    #[test]
    fn deserialize_non_ascii() {
        let bytes = vec![
            13, 0, 0, 0, 0, 0, 0, 0, 230, 151, 165, 230, 156, 172, 232, 170, 158, 240, 159, 146,
            150, 1, 16, 0, 0, 0, 0, 0, 0, 0, 209, 128, 208, 182, 208, 176, 208, 178, 209, 135, 208,
            184, 208, 189, 208, 176,
        ];
        let pair = InternalPair::deserialize(&bytes).unwrap();
        assert_eq!(InternalPair::new(("日本語💖", Some("ржавчина"))), pair);
    }

    #[test]
    fn ordering() {
        let pair1 = InternalPair::new(("abc", Some("defg")));
        let pair2 = InternalPair::new(("日本語💖", Some("ржавчина")));
        assert!(pair1 < pair2);
    }
}
