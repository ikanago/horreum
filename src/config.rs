use std::path::PathBuf;
use structopt::StructOpt;

/// Structure for app configuration.
#[derive(Debug, StructOpt)]
#[structopt(
    name = "horreum",
    author = "ikanago",
    about = "Persistent key-value store."
)]
pub struct Config {
    /// Port number server listens to.
    #[structopt(
        short,
        long,
        default_value = "8080",
        help = "Number of threads to handle requests"
    )]
    pub port: u16,

    /// Directory to store SSTable's files.
    #[structopt(
        short,
        long,
        default_value = "horreum_data",
        parse(from_os_str),
        help = "Directory storing SSTable files"
    )]
    pub directory: PathBuf,

    /// Every `block_stride` pair, `SSTable` creates an index entry.
    #[structopt(
        short = "s",
        long = "stride",
        default_value = "10",
        help = "Size of block of SSTable index"
    )]
    pub block_stride: usize,
}
