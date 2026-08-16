use super::WalArtifact;
use crate::config::NldbConfig;
use crate::memtable::inner::MemtableInner;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

pub fn read_immutable_tables(
    wal_files: &[WalArtifact],
    config: &NldbConfig,
) -> VecDeque<Arc<MemtableInner>> {
    wal_files
        .par_iter()
        .map(|w| {
            let t = load_immutable_memtable_worker(
                &w.filename,
                config.max_memtable_size as usize,
                config.max_memtable_nodes as usize,
            )
            .expect("Unable to load immutable memtable form full wal");
            Arc::new(t)
        })
        .collect()
}

fn load_immutable_memtable_worker(
    path: &Path,
    max_size: usize,
    max_nodes: usize,
) -> std::io::Result<MemtableInner> {
    MemtableInner::from_wal(path, max_size, max_nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NldbConfig;
    use crate::memtable::inner::{MemtableNode, MemtableQuery, NodeData};
    use crate::wal::Wal;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn config() -> NldbConfig {
        NldbConfig {
            max_memtable_size: usize::MAX as u64,
            max_memtable_nodes: 64,
            compaction_rate: 10,
            cache_size: 100,
            ..Default::default()
        }
    }

    struct WalGuard(PathBuf);
    impl Drop for WalGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    // Creates a WAL file with the given records. The ts value is embedded in the
    // filename so WalArtifact::new can parse it. Returns the artifact and a guard
    // that cleans up the file if the test panics before from_wal deletes it.
    fn make_wal(ts: u128, records: &[(&[u8], &[u8])]) -> (WalArtifact, WalGuard) {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let thread_id = std::thread::current().id();
        // Format: "test_rworker_{thread_id}_{id}_wal.{ts}.log"
        // split('.') → ["test_rworker_..._wal", "{ts}", "log"] → nth(1) = ts ✓
        let path: PathBuf = format!("test_rworker_{thread_id:?}_{id}_wal.{ts}.log").into();

        let fd = fs::File::create(&path).unwrap();
        let mut wal = Wal::from_fd_and_path(fd, path.clone());
        for (key, value) in records {
            let node = MemtableNode::new_for_test(key.to_vec(), NodeData::Data(value.to_vec()));
            wal.write_log(&node);
        }
        wal.flush_for_test();

        let artifact = WalArtifact::new(path.clone());
        (artifact, WalGuard(path))
    }

    #[test]
    fn test_single_wal_data_recovered() {
        let (artifact, guard) = make_wal(1000, &[(b"foo", b"bar"), (b"baz", b"qux")]);
        let path = artifact.filename.clone();

        let result = read_immutable_tables(&[artifact], &config());

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get(b"foo"), MemtableQuery::Data(b"bar".to_vec()));
        assert_eq!(result[0].get(b"baz"), MemtableQuery::Data(b"qux".to_vec()));
        assert!(!path.exists());
        for table in result {
            let _ = fs::remove_file(&table.wal.filename);
        }
        std::mem::forget(guard);
    }

    #[test]
    fn test_empty_wal_produces_empty_table() {
        let (artifact, guard) = make_wal(2000, &[]);
        let path = artifact.filename.clone();

        let result = read_immutable_tables(&[artifact], &config());

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get(b"anything"), MemtableQuery::None);
        assert!(!path.exists());
        for table in result {
            let _ = fs::remove_file(&table.wal.filename);
        }
        std::mem::forget(guard);
    }

    #[test]
    fn test_multiple_wals_all_loaded() {
        let (a1, g1) = make_wal(1000, &[(b"k1", b"v1")]);
        let (a2, g2) = make_wal(2000, &[(b"k2", b"v2")]);
        let (a3, g3) = make_wal(3000, &[(b"k3", b"v3")]);
        let p1 = a1.filename.clone();
        let p2 = a2.filename.clone();
        let p3 = a3.filename.clone();

        let result = read_immutable_tables(&[a1, a2, a3], &config());

        assert_eq!(result.len(), 3);
        assert!(!p1.exists());
        assert!(!p2.exists());
        assert!(!p3.exists());
        for table in result {
            let _ = fs::remove_file(&table.wal.filename);
        }
        std::mem::forget((g1, g2, g3));
    }

    #[test]
    fn test_order_preserved() {
        // WALs passed in oldest-first; result VecDeque must preserve that order.
        let (a1, g1) = make_wal(1000, &[(b"only_in_first", b"yes")]);
        let (a2, g2) = make_wal(2000, &[(b"only_in_second", b"yes")]);

        let result = read_immutable_tables(&[a1, a2], &config());

        assert_eq!(result.len(), 2);
        // Index 0 = oldest, index 1 = newest.
        assert_eq!(
            result[0].get(b"only_in_first"),
            MemtableQuery::Data(b"yes".to_vec())
        );
        assert_eq!(result[0].get(b"only_in_second"), MemtableQuery::None);
        assert_eq!(
            result[1].get(b"only_in_second"),
            MemtableQuery::Data(b"yes".to_vec())
        );
        assert_eq!(result[1].get(b"only_in_first"), MemtableQuery::None);
        for table in result {
            let _ = fs::remove_file(&table.wal.filename);
        }
        std::mem::forget((g1, g2));
    }

    #[test]
    fn test_tables_are_independent() {
        // Each WAL is a separate memtable; overlapping keys are not merged.
        let (a1, g1) = make_wal(1000, &[(b"k", b"old")]);
        let (a2, g2) = make_wal(2000, &[(b"k", b"new")]);

        let result = read_immutable_tables(&[a1, a2], &config());

        assert_eq!(result[0].get(b"k"), MemtableQuery::Data(b"old".to_vec()));
        assert_eq!(result[1].get(b"k"), MemtableQuery::Data(b"new".to_vec()));
        for table in result {
            let _ = fs::remove_file(&table.wal.filename);
        }
        std::mem::forget((g1, g2));
    }
}
