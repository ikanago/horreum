use std::collections::HashMap;

fn parse_request(url: &str) -> HashMap<String, String> {
    let queries = HashMap::new();
    queries
}

#[cfg(test)]
mod tests {
    use crate::http::parse::*;

    #[test]
    fn test_retrieve_query() {
        let expected: HashMap<String, String> = Vec::from([("key".to_string(), "abc".to_string())])
            .into_iter()
            .collect();
        let actual = parse_request("/?key=abc");
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_retrieve_multiple_queries() {
        let expected: HashMap<String, String> = Vec::from([
            ("key".to_string(), "abc".to_string()),
            ("value".to_string(), "def".to_string()),
        ])
        .into_iter()
        .collect();
        let actual = parse_request("/?key=abc&value=def");
        assert_eq!(expected, actual);
    }
}
