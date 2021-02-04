use horreum::{serve, Config, MemTable, SSTableManager};
use structopt::StructOpt;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let (memtable_tx, memtable_rx) = mpsc::channel(32);
    let (sstable_tx, sstable_rx) = mpsc::channel(32);
    let config = Config::from_args();
    dbg!(&config);
    let mut memtable = MemTable::new(config.memtable_limit, memtable_rx, sstable_tx.clone());
    let mut manager =
        match SSTableManager::new(config.directory, config.block_stride, sstable_rx).await {
            Ok(m) => m,
            Err(err) => {
                eprintln!("{}", err);
                std::process::exit(1);
            }
        };

    tokio::spawn(async move { memtable.listen().await });
    tokio::spawn(async move { manager.listen().await });
    serve(config.port, memtable_tx, sstable_tx).await?;
    Ok(())
}
