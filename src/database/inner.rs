use crate::compaction::{CompactionSignal, SSTableLoadAck, sstable_background};
use crate::config::NldbConfig;
use crate::error::MemtableError;
use crate::memtable::{
    Memtable,
    immutable::ImmutableMemtable,
    inner::{Blob, MemtableFlushSignal, MemtableQuery, NodeData, flush_memtable},
};
use crate::restart::{SSTableArtifact, WalArtifact};
use crate::sstable::SSTableCache;
use dash_cache::DashCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, atomic::AtomicBool};
use tokio::sync::mpsc::{self, Sender};

fn construct_memtable(live_wal: Option<WalArtifact>, config: &NldbConfig) -> Memtable {
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
    config: &NldbConfig,
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
/// Type is wrapped with Arc in the publicly exposed type, `[super::Nldb]`.
#[derive(Debug)]
pub(super) struct NldbInner {
    memtable: Memtable,
    sstable_cache: SSTableCache,
    cache: DashCache<Blob, Blob>,
    poisoned: Arc<AtomicBool>,
    signal: Sender<CompactionSignal>,
    memtable_channel: Sender<MemtableFlushSignal>,
    immutable: ImmutableMemtable,
}

impl NldbInner {
    /// Construct a new in memory database instance. If there is existing state, the "restart" path
    /// is used to read in current state, otherwise, an empty state is created.
    pub(super) fn new(
        sstable_files: Vec<SSTableArtifact>,
        mut wal_files: Vec<WalArtifact>,
        config: &NldbConfig,
    ) -> NldbInner {
        let memtable = construct_memtable(wal_files.pop(), config);

        let cache: DashCache<Blob, Blob> = DashCache::with_num_shards_and_capacity(
            NonZeroUsize::new(config.cache_shards as usize).unwrap(),
            NonZeroUsize::new(config.cache_size as usize).unwrap(),
        );

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

        NldbInner {
            memtable,
            sstable_cache,
            cache,
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
        let cached = self.cache.get(key).await;

        if cached.is_some() {
            return cached;
        }

        if let Some(blob) = self.read_memtable(&key).await {
            self.read_through_cache(key.to_owned(), blob.clone()).await;
            return Some(blob);
        }

        if let Some(blob) = self.read_immutable_table(&key).await {
            self.read_through_cache(key.to_owned(), blob.clone()).await;
            return Some(blob);
        }

        if let Ok(value) = self.sstable_cache.search(&key).await {
            self.read_through_cache(key.to_owned(), value.clone()).await;
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

    async fn read_through_cache(&self, key: Blob, value: Blob) {
        self.cache.insert(key, value.clone()).await;
    }

    // Evict a key if cached on a insert/delete. DashCache internally handles the evict only if it
    // exists.
    #[inline]
    async fn evict_if_cached(&self, key: &[u8]) {
        let _ = self.cache.evict(key).await;
    }

    pub(super) async fn delete(&self, key: Blob) {
        self.check_poison_flag();
        self.evict_if_cached(&key).await;
        match self.memtable.delete(key).await {
            Ok(_) => {}
            Err(e) => {
                let MemtableError::TableFull(key, value) = e;
                self.rotate_table().await;
                // SAFETY: We now know the memtable has space.
                match value {
                    NodeData::Data(_) => {
                        unreachable!()
                    }
                    NodeData::Tombstone => {
                        let _ = self.memtable.delete(key).await;
                    }
                }
            }
        }
    }

    pub(super) async fn write(&self, key: Blob, value: Blob) {
        self.check_poison_flag();
        self.evict_if_cached(&key).await;
        match self.memtable.insert(key, value).await {
            Ok(_) => (),
            Err(e) => {
                let MemtableError::TableFull(key, value) = e;
                self.rotate_table().await;
                // SAFETY: We now know the memtable has space.
                match value {
                    NodeData::Data(blob) => {
                        let _ = self.memtable.insert(key, blob).await;
                    }
                    NodeData::Tombstone => {
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

impl Drop for NldbInner {
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
