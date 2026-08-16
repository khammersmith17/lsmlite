use crate::compaction::{CompactionSignal, SSTableLoadAck, sstable_background};
use crate::config::LsmliteConfig;
use crate::error::MemtableError;
use crate::memtable::{
    Memtable,
    immutable::ImmutableMemtable,
    inner::{Blob, MemtableFlushSignal, MemtableQuery, flush_memtable},
};
use crate::restart::{SSTableArtifact, WalArtifact};
use crate::sstable::SSTableCache;
use std::sync::{Arc, atomic::AtomicBool};
use tokio::sync::mpsc::{self, Sender};

fn construct_memtable(live_wal: Option<WalArtifact>, config: &LsmliteConfig) -> Memtable {
    if let Some(wal) = live_wal {
        Memtable::new_from_wal(
            &wal.filename,
            config.max_memtable_size as usize,
            config.max_memtable_nodes as usize,
        )
        .expect("Unable to create memtable from WAL file")
    } else {
        Memtable::new(
            config.max_memtable_size as usize,
            config.max_memtable_nodes as usize,
        )
        .expect("Unable to create memtable")
    }
}

fn construct_sstable_cache(
    sstable_files: Vec<SSTableArtifact>,
    config: &LsmliteConfig,
) -> SSTableCache {
    if sstable_files.is_empty() {
        SSTableCache::new(config.compaction_rate as usize)
    } else {
        SSTableCache::from_restart(sstable_files, config.compaction_rate as usize)
    }
}

/// Inner memtable struct that holds all database implementation.
/// On write/delete, if that memtable is full, it will return an error. On
/// [MemtableError::TableFull], the table will be rotated out while being flushed.
///
/// Type is wrapped with Arc in the publicly exposed type, `[super::LsmLite]`.
#[derive(Debug)]
pub(super) struct LsmliteInner {
    memtable: Memtable,
    sstable_cache: SSTableCache,
    poisoned: Arc<AtomicBool>,
    signal: Sender<CompactionSignal>,
    memtable_channel: Sender<MemtableFlushSignal>,
    immutable: ImmutableMemtable,
}

impl LsmliteInner {
    /// Construct a new in memory database instance. If there is existing state, the "restart" path
    /// is used to read in current state, otherwise, an empty state is created.
    pub(super) fn new(
        sstable_files: Vec<SSTableArtifact>,
        mut wal_files: Vec<WalArtifact>,
        config: &LsmliteConfig,
    ) -> LsmliteInner {
        let memtable = construct_memtable(wal_files.pop(), config);

        let sstable_cache = construct_sstable_cache(sstable_files, config);
        let poisoned = Arc::new(AtomicBool::new(false));

        let (signal, receiver) =
            mpsc::channel::<CompactionSignal>(config.compaction_queue_size as usize);

        let (memtable_channel, table_flush_recv) =
            mpsc::channel::<MemtableFlushSignal>(config.memtable_flush_queue_size as usize);

        let (sstable_ack_tx, sstable_ack_rx) = mpsc::channel::<SSTableLoadAck>(1);
        let immutable = ImmutableMemtable::new(wal_files, config, memtable_channel.clone());

        let _compaction_handle = tokio::task::spawn(sstable_background(
            receiver,
            sstable_cache.clone(),
            immutable.clone(),
            sstable_ack_tx,
        ));

        let shared_immutable = immutable.clone();
        let shared_flag = poisoned.clone();
        let compaction_signal = signal.clone();

        let _memtable_flush_handle = tokio::task::spawn(flush_memtable(
            shared_immutable,
            compaction_signal,
            shared_flag,
            table_flush_recv,
            sstable_ack_rx,
        ));

        LsmliteInner {
            memtable,
            sstable_cache,
            poisoned,
            signal,
            immutable,
            memtable_channel,
        }
    }

    // First check cache.
    // Second check memtable.
    // Third check immutable table being flushed to disk.
    // Fourth check SSTables.
    pub(super) async fn get(&self, key: &[u8]) -> Option<Blob> {
        self.check_poison_flag();

        if let Some(blob) = self.read_memtable(&key).await {
            return Some(blob);
        }

        if let Some(blob) = self.read_immutable_table(&key).await {
            return Some(blob);
        }

        if let Ok(value) = self.sstable_cache.search(&key).await {
            return Some(value);
        }

        None
    }

    async fn read_memtable(&self, key: &[u8]) -> Option<Blob> {
        match self.memtable.get(key).await {
            MemtableQuery::Data(blob) => Some(blob),
            MemtableQuery::Tombstone => None,
            _ => None,
        }
    }

    async fn read_immutable_table(&self, key: &[u8]) -> Option<Blob> {
        match self.immutable.get(key).await {
            MemtableQuery::Data(blob) => Some(blob),
            MemtableQuery::Tombstone => None,
            _ => None,
        }
    }

    pub(super) async fn delete(&self, key: Blob) {
        self.check_poison_flag();
        match self.memtable.delete(key).await {
            Ok(_) => {}
            Err(e) => {
                let MemtableError::TableFull(key, value) = e;
                self.rotate_table().await;
                // SAFETY: We now know the memtable has space.
                match value {
                    Some(_) => {
                        unreachable!()
                    }
                    None => {
                        let _ = self.memtable.delete(key).await;
                    }
                }
            }
        }
    }

    pub(super) async fn set(&self, key: Blob, value: Blob) {
        self.check_poison_flag();
        match self.memtable.insert(key, value).await {
            Ok(_) => (),
            Err(e) => {
                let MemtableError::TableFull(key, value) = e;
                self.rotate_table().await;
                // SAFETY: We now know the memtable has space.
                match value {
                    Some(blob) => {
                        let _ = self.memtable.insert(key, blob).await;
                    }
                    None => {
                        unreachable!()
                    }
                }
            }
        }
    }

    async fn rotate_table(&self) {
        let full_table = self
            .memtable
            .rotate()
            .await
            .expect("Unable to create fresh memtable on rotate");

        self.immutable.insert(full_table).await;
        self.memtable_channel
            .send(MemtableFlushSignal::Flush)
            .await
            .expect("Unable to send memtable flush signal");
    }

    fn check_poison_flag(&self) {
        let flag = self.poisoned.load(std::sync::atomic::Ordering::Acquire);
        if flag {
            panic!("Database reached poisoned state")
        }
    }
}

impl Drop for LsmliteInner {
    fn drop(&mut self) {
        // When this is dropped, signal to the compaction and memtable flush background threads
        // to exit, to gracefully handle shutdown on exit.
        self.signal
            .blocking_send(CompactionSignal::Shutdown)
            .expect("Unable to send compaction shutdown signal");

        self.memtable_channel
            .blocking_send(MemtableFlushSignal::Shutdown)
            .expect("Unable to send memtable flush shutdown signal");
    }
}
