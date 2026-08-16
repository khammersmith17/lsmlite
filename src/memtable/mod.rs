use std::sync::Arc;
use tokio::sync::RwLock;
pub mod immutable;
pub(crate) mod inner;
pub(crate) mod record;
use crate::error::MemtableError;
use inner::{Blob, MemtableInner, MemtableQuery};
use std::path::Path;

#[derive(Debug)]
pub struct Memtable {
    inner: Arc<RwLock<MemtableInner>>,
    max_size: usize,
    max_nodes: usize,
}

impl Memtable {
    pub fn new(max_size: usize, max_nodes: usize) -> std::io::Result<Memtable> {
        let inner = Arc::new(RwLock::new(MemtableInner::new(max_size, max_nodes)?));
        Ok(Memtable {
            inner,
            max_size,
            max_nodes,
        })
    }

    pub fn new_from_wal(
        wal_filepath: &Path,
        max_size: usize,
        max_nodes: usize,
    ) -> std::io::Result<Memtable> {
        let inner_table = MemtableInner::from_wal(wal_filepath, max_size, max_nodes)?;
        let inner = Arc::new(RwLock::new(inner_table));

        Ok(Memtable {
            inner,
            max_size,
            max_nodes,
        })
    }

    pub async fn insert(&self, key: Blob, data: Blob) -> Result<(), MemtableError> {
        let mut handle = self.inner.write().await;
        handle.insert_data_record(key, data)?;
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> MemtableQuery {
        let handle = self.inner.read().await;
        handle.get(key)
    }

    pub async fn delete(&self, key: Blob) -> Result<(), MemtableError> {
        let mut handle = self.inner.write().await;
        handle.insert_tombstone_record(key)
    }

    pub async fn rotate(&self) -> std::io::Result<MemtableInner> {
        let mut handle = self.inner.write().await;
        let full_table = std::mem::replace(
            &mut *handle,
            MemtableInner::new(self.max_size, self.max_nodes)?,
        );
        Ok(full_table)
    }

    #[cfg(test)]
    pub async fn wal_path_for_test(&self) -> std::path::PathBuf {
        self.inner.read().await.wal.filename.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::inner::MemtableQuery;
    use crate::wal::Wal;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct WalGuard(PathBuf);
    impl Drop for WalGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn make_memtable() -> (Memtable, WalGuard) {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let thread_id = std::thread::current().id();
        let wal_path: PathBuf = format!("test_memtable_mod_{thread_id:?}_{id}.log").into();
        let fd = fs::File::create(&wal_path).unwrap();
        let wal = Wal::from_fd_and_path(fd, wal_path.clone());
        let inner = MemtableInner {
            arena: Vec::with_capacity(64),
            max_size: usize::MAX,
            root_node: None,
            current_size: 0,
            wal,
        };
        let memtable = Memtable {
            inner: Arc::new(RwLock::new(inner)),
            max_size: usize::MAX,
            max_nodes: 64,
        };
        (memtable, WalGuard(wal_path))
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let (m, _g) = make_memtable();
        m.insert(b"foo".to_vec(), b"bar".to_vec()).await.unwrap();
        assert_eq!(m.get(b"foo").await, MemtableQuery::Data(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn test_get_missing_returns_none() {
        let (m, _g) = make_memtable();
        assert_eq!(m.get(b"missing").await, MemtableQuery::None);
    }

    #[tokio::test]
    async fn test_insert_overwrites_existing() {
        let (m, _g) = make_memtable();
        m.insert(b"k".to_vec(), b"v1".to_vec()).await.unwrap();
        m.insert(b"k".to_vec(), b"v2".to_vec()).await.unwrap();
        assert_eq!(m.get(b"k").await, MemtableQuery::Data(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_delete_writes_tombstone() {
        let (m, _g) = make_memtable();
        m.insert(b"k".to_vec(), b"v".to_vec()).await.unwrap();
        m.delete(b"k".to_vec()).await.unwrap();
        assert_eq!(m.get(b"k").await, MemtableQuery::Tombstone);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_key_writes_tombstone() {
        let (m, _g) = make_memtable();
        m.delete(b"ghost".to_vec()).await.unwrap();
        assert_eq!(m.get(b"ghost").await, MemtableQuery::Tombstone);
    }

    #[tokio::test]
    async fn test_rotate_returns_old_table() {
        let (m, _g) = make_memtable();
        m.insert(b"a".to_vec(), b"1".to_vec()).await.unwrap();
        m.insert(b"b".to_vec(), b"2".to_vec()).await.unwrap();

        let old = m.rotate().await.unwrap();
        // rotate() installed a fresh MemtableInner with a new WAL; clean it up.
        let fresh_wal = m.wal_path_for_test().await;
        assert_eq!(old.get(b"a"), MemtableQuery::Data(b"1".to_vec()));
        assert_eq!(old.get(b"b"), MemtableQuery::Data(b"2".to_vec()));
        let _ = fs::remove_file(&fresh_wal);
    }

    #[tokio::test]
    async fn test_rotate_fresh_table_is_empty() {
        let (m, _g) = make_memtable();
        m.insert(b"a".to_vec(), b"1".to_vec()).await.unwrap();
        m.rotate().await.unwrap();
        // rotate() installed a fresh MemtableInner with a new WAL; clean it up.
        let fresh_wal = m.wal_path_for_test().await;
        assert_eq!(m.get(b"a").await, MemtableQuery::None);
        let _ = fs::remove_file(&fresh_wal);
    }

    #[tokio::test]
    async fn test_table_full_error() {
        // max_nodes=2: root + one child fill capacity, third insert errors.
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let thread_id = std::thread::current().id();
        let wal_path: PathBuf = format!("test_memtable_full_{thread_id:?}_{id}.log").into();
        let _g = WalGuard(wal_path.clone());
        let fd = fs::File::create(&wal_path).unwrap();
        let wal = Wal::from_fd_and_path(fd, wal_path.clone());
        let inner = MemtableInner {
            arena: Vec::with_capacity(2),
            max_size: usize::MAX,
            root_node: None,
            current_size: 0,
            wal,
        };
        let m = Memtable {
            inner: Arc::new(RwLock::new(inner)),
            max_size: usize::MAX,
            max_nodes: 2,
        };
        m.insert(b"a".to_vec(), b"".to_vec()).await.unwrap();
        m.insert(b"b".to_vec(), b"".to_vec()).await.unwrap();
        assert!(matches!(
            m.insert(b"c".to_vec(), b"".to_vec()).await,
            Err(MemtableError::TableFull(..))
        ));
    }

    #[tokio::test]
    async fn test_new_from_wal_recovers_data() {
        use crate::memtable::inner::MemtableNode;

        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let thread_id = std::thread::current().id();
        let wal_path: PathBuf = format!("test_memtable_wal_recover_{thread_id:?}_{id}.log").into();

        // Write records directly to a WAL without constructing a MemtableInner.
        {
            let fd = fs::File::create(&wal_path).unwrap();
            let mut wal = Wal::from_fd_and_path(fd, wal_path.clone());
            wal.write_log(&MemtableNode::new_for_test(b"hello".to_vec(), b"world".to_vec()));
            wal.write_log(&MemtableNode::new_for_test(b"foo".to_vec(), b"bar".to_vec()));
            wal.flush_for_test();
        }

        let m = Memtable::new_from_wal(&wal_path, usize::MAX, 64).unwrap();
        // Original WAL deleted by new_from_wal; clean up the new WAL created internally.
        assert!(!wal_path.exists());
        let new_wal = m.wal_path_for_test().await;
        assert_eq!(
            m.get(b"hello").await,
            MemtableQuery::Data(b"world".to_vec())
        );
        assert_eq!(m.get(b"foo").await, MemtableQuery::Data(b"bar".to_vec()));
        assert_eq!(m.get(b"missing").await, MemtableQuery::None);
        let _ = fs::remove_file(&new_wal);
    }
}
