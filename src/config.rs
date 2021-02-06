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
        help = "Port number server listens to"
    )]
    pub port: u16,

    /// Limit of MemTable size to flush its contents.
    #[structopt(
        long,
        default_value = "4096",
        help = "Limit of MemTable size to flush its contents"
    )]
    pub memtable_limit: usize,

    /// Directory to store SSTable's files.
    #[structopt(
        short,
        long,
        default_value = "horreum_data",
        parse(from_os_str),
        help = "Directory to store SSTable files"
    )]
    pub directory: PathBuf,

    #[structopt(
        short = "r",
        long = "compaction_ratio",
        help = "Compaction trigger ratio in percentage"
    )]
    pub compaction_trigger_ratio: u64,

    /// Every `block_stride` pair, `SSTable` creates an index entry.
    #[structopt(
        short = "s",
        long = "stride",
        default_value = "10",
        help = "Size of block of SSTable index"
    )]
    pub block_stride: usize,
}
