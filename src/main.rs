pub mod cli;
pub mod compaction;
pub mod config;
pub mod constants;
pub mod database;
pub mod disk;
pub mod error;
pub mod memtable;
pub mod parser;
pub mod restart;
pub mod ser;
pub mod ssindex;
pub mod sstable;
pub mod tcp_server;
pub mod util;
pub mod wal;

#[tokio::main]
async fn main() {
    let Ok(config) = cli::parse_config() else {
        panic!("Provided database config is invalid");
    };
    let (sstable_state, wal_state) = restart::get_restart_state();
    let db = database::Nldb::new(sstable_state, wal_state, &config);
    tcp_server::run(db).await;
}
