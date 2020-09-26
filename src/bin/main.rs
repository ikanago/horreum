use horreum::http;
use horreum::index::Horreum;

fn main() {
    let db = Horreum::new();
    http::listen(&db, 4);
    // let pairs = vec![
    //     ("hoge", "fuga"),
    //     ("neko", "cat"),
    //     ("nya", "meow"),
    //     ("rust", "safe"),
    // ];
    // let pairs: Vec<(Vec<u8>, Vec<u8>)> = pairs
    //     .iter()
    //     .map(|&(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
    //     .collect();

    // let mut handles = Vec::new();
    // for (key, value) in &pairs {
    //     handles.push(thread::spawn(|| {
    //         db.put(key.clone(), value.clone());
    //     }));
    // }
    // for handle in handles {
    //     handle.join().unwrap();
    // }

    // for (key, _) in pairs {
    //     thread::spawn(|| {
    //         db.get(&key);
    //     });
    // }
}
