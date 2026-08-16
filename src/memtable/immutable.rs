use super::inner::{MemtableFlushSignal, MemtableInner, MemtableQuery, flush_memtable_inner};
use crate::config::LsmliteConfig;
use crate::restart::{WalArtifact, worker::read_immutable_tables};
use std::collections::VecDeque;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;

#[derive(Clone, Debug)]
pub struct ImmutableMemtable {
    table: Arc<RwLock<VecDeque<Arc<MemtableInner>>>>,
}

impl ImmutableMemtable {
    /// Newest wal file is assumed to not be present in this list.
    /// Wal files passed here are assumed to be waiting for SSTable flush.
    /// Thus, they get loaded into the immutable in memory queue.
    pub fn new(
        wal_files: Vec<WalArtifact>,
        config: &LsmliteConfig,
        flusher_signal: Sender<MemtableFlushSignal>,
    ) -> ImmutableMemtable {
        if wal_files.is_empty() {
            return Self::new_empty();
        }
        let n_files = wal_files.len();
        let table_inner = read_immutable_tables(&wal_files, config);
        let table = Arc::new(RwLock::new(table_inner));

        for _ in 0..n_files {
            flusher_signal
                .blocking_send(MemtableFlushSignal::Flush)
                .expect("Unable to send flush signal to compaction worker");
        }

        ImmutableMemtable { table }
    }

    pub fn new_empty() -> ImmutableMemtable {
        let table = Arc::new(RwLock::new(VecDeque::new()));
        ImmutableMemtable { table }
    }

    pub async fn pop(&self) {
        let mut handle = self.table.write().await;
        handle.pop_front();
    }

    pub async fn get(&self, key: &[u8]) -> MemtableQuery {
        let handle = self.table.read().await;
        for inner in handle.iter().rev() {
            let result = inner.get(key);
            match result {
                MemtableQuery::Data(_) => return result,
                MemtableQuery::Tombstone => return MemtableQuery::Tombstone,
                _ => {}
            }
        }

        MemtableQuery::None
    }

    pub async fn insert(&self, table: MemtableInner) {
        let mut handle = self.table.write().await;
        handle.push_back(table.into());
    }

    // This is only called by the background thread. Is does not block readers as this is held by a
    // read lock. Short scoped read lock is the only contention here between any threads.
    pub async fn flush(&self, fd: &mut File) -> std::io::Result<()> {
        let head = {
            let handle = self.table.read().await;

            Arc::clone(
                handle
                    .front()
                    .expect("Flush on immutable table without any tables in the queue"),
            )
        };

        flush_memtable_inner(fd, head)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::inner::{MemtableInner, MemtableQuery};
    use std::fs;
    use std::path::PathBuf;

    struct WalGuard(PathBuf);
    impl Drop for WalGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn make_inner(entries: &[(&str, Option<&[u8]>)]) -> (MemtableInner, WalGuard) {
        let (mut inner, path) = MemtableInner::new_for_test();
        for (key, data) in entries {
            match data {
                Some(v) => inner.insert_data_record(key.as_bytes().to_vec(), v.to_vec()).unwrap(),
                None => inner.insert_tombstone_record(key.as_bytes().to_vec()).unwrap(),
            }
        }
        (inner, WalGuard(path))
    }

    #[tokio::test]
    async fn test_empty_returns_none() {
        let imm = ImmutableMemtable::new_empty();
        assert_eq!(imm.get(b"key").await, MemtableQuery::None);
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let imm = ImmutableMemtable::new_empty();
        let (inner, _guard) = make_inner(&[("k", Some(b"v"))]);
        imm.insert(inner).await;
        assert_eq!(imm.get(b"k").await, MemtableQuery::Data(b"v".to_vec()));
        assert_eq!(imm.get(b"missing").await, MemtableQuery::None);
    }

    #[tokio::test]
    async fn test_newest_wins() {
        let imm = ImmutableMemtable::new_empty();
        let (inner_a, _ga) = make_inner(&[("k", Some(b"v1"))]);
        let (inner_b, _gb) = make_inner(&[("k", Some(b"v2"))]);
        imm.insert(inner_a).await; // older
        imm.insert(inner_b).await; // newer
        assert_eq!(imm.get(b"k").await, MemtableQuery::Data(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_tombstone_hides_older_data() {
        let imm = ImmutableMemtable::new_empty();
        let (inner_a, _ga) = make_inner(&[("k", Some(b"v1"))]);
        let (inner_b, _gb) = make_inner(&[("k", None)]);
        imm.insert(inner_a).await; // older
        imm.insert(inner_b).await; // newer tombstone
        assert_eq!(imm.get(b"k").await, MemtableQuery::Tombstone);
    }

    #[tokio::test]
    async fn test_pop_removes_oldest() {
        let imm = ImmutableMemtable::new_empty();
        let (inner_a, _ga) = make_inner(&[("only_a", Some(b"v"))]);
        let (inner_b, _gb) = make_inner(&[("only_b", Some(b"v"))]);
        imm.insert(inner_a).await; // older
        imm.insert(inner_b).await; // newer
        imm.pop().await;
        assert_eq!(imm.get(b"only_a").await, MemtableQuery::None);
        assert_eq!(imm.get(b"only_b").await, MemtableQuery::Data(b"v".to_vec()));
    }
}
