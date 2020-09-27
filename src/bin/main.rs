use horreum::http;
use horreum::index::Horreum;

fn main() {
    let db = Horreum::new();
    http::listen(&db, 4);
}
