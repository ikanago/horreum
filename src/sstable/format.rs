use bincode::serialize;
use serde::Serialize;

#[derive(Clone, Debug)]
pub enum ValueType {
    Deleted,
    Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct InternalPair<'a> {
    key: &'a [u8],
    value: Option<&'a [u8]>,
}

impl<'a> InternalPair<'a> {
    pub fn new(pair: (&'a str, Option<&'a str>)) -> Self {
        Self {
            key: pair.0.as_bytes(),
            value: pair.1.map(|v| v.as_bytes()),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        serialize(self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_normal_pair() {
        let data = ("abc", Some("defg"));
        let pair = InternalPair::new(data);
        assert_eq!(
            vec![
                3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 1, 4, 0, 0, 0, 0, 0, 0, 0, 100, 101, 102, 103,
            ],
            pair.serialize()
        );
    }

    #[test]
    fn serialize_lacking_value() {
        let data = ("abc", None);
        let pair = InternalPair::new(data);
        assert_eq!(
            vec![3, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 0,],
            pair.serialize()
        );
    }

    #[test]
    fn serialize_non_ascii() {
        let data = ("日本語💖", Some("ржавчина"));
        let pair = InternalPair::new(data);
        assert_eq!(
            vec![
                13, 0, 0, 0, 0, 0, 0, 0, 230, 151, 165, 230, 156, 172, 232, 170, 158, 240, 159,
                146, 150, 1, 16, 0, 0, 0, 0, 0, 0, 0, 209, 128, 208, 182, 208, 176, 208, 178, 209,
                135, 208, 184, 208, 189, 208, 176,
            ],
            pair.serialize()
        );
    }
}
