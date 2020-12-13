use clap::{clap_app, crate_version};
use horreum::{Horreum, http};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = clap_app!(horreum =>
        (version: crate_version!())
        (@arg PORT: -p --port +takes_value "Number of threads to handle requests")
        (@arg STRIDE: -s --stride +takes_value "Size of block of SSTable index")
        (@arg DIR: -d --dir +takes_value "Directory storing SSTable files")
    )
    .get_matches();

    let port = matches.value_of("PORT").unwrap_or("8080");
    let port = port.parse::<u16>().unwrap();
    let sstable_directory = if let Some(directory) = matches.value_of("DIR") {
        directory
    } else {
        "test_main"
    };
    let block_stride = matches.value_of("STRIDE").unwrap_or("100");
    let block_stride = block_stride.parse::<usize>().unwrap();

    let db = Horreum::new(sstable_directory, block_stride).await?;
    http::serve(&db, port).await?;
    Ok(())
}
