use horreum::{serve, Config, MemTable, SSTableManager};
use structopt::StructOpt;
use tokio::sync::mpsc;
use crossbeam_channel::unbounded;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let (memtable_tx, memtable_rx) = mpsc::channel(32);
    let (sstable_tx, sstable_rx) = unbounded();
    let (flushing_tx, flushing_rx) = unbounded();
    let config = Config::from_args();
    dbg!(&config);
    let mut memtable = MemTable::new(config.memtable_limit, memtable_rx, flushing_tx);
    let mut manager = match SSTableManager::new(
        config.directory,
        config.block_stride,
        sstable_rx,
        flushing_rx,
    )
    .await
    {
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
