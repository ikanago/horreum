use clap::{clap_app, crate_version};
use horreum::{horreum::Horreum, http};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = clap_app!(horreum =>
        (version: crate_version!())
        (@arg PORT: -p --port +takes_value "Number of threads to handle requests")
    )
    .get_matches();

    let port = matches.value_of("PORT").unwrap_or("8080");
    let port = port.parse::<u16>().unwrap();

    let sstable_directory = "test_main";
    let block_stride = 3;
    let db = Horreum::new(sstable_directory, block_stride).await?;
    http::serve(&db, port).await?;
    Ok(())
}
