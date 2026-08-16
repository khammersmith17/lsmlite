use crate::disk::DiskRecord;
use crate::memtable::{immutable::ImmutableMemtable, inner::NodeData};
use crate::sstable::iterator::SSTableIterator;
use crate::sstable::{SSTable, SSTableCache, compaction_writer::CompactionWriter};
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{read_dir, remove_file};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};

pub enum CompactionSignal {
    Shutdown,
    LoadSSTable(PathBuf),
}

pub enum SSTableLoadAck {
    Done,
}

#[derive(PartialEq, Eq)]
struct HeapNode {
    record: DiskRecord,
    table_idx: usize,
}

impl PartialOrd for HeapNode {
    // First compare on key.
    // If key is the same, the large key wins here (Reverse in BinaryHeap implementation).
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.record.cmp(&other.record))
    }
}

impl Ord for HeapNode {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.record.cmp(&other.record);
        match ord {
            Ordering::Equal => self.table_idx.cmp(&other.table_idx),
            _ => ord,
        }
    }
}

impl HeapNode {
    fn new(record: DiskRecord, table_idx: usize) -> HeapNode {
        HeapNode { record, table_idx }
    }
}

async fn tables_to_iterators(tables: Vec<Arc<Mutex<SSTable>>>) -> Vec<SSTableIterator> {
    let mut iters = Vec::with_capacity(tables.len());
    for table in tables.into_iter() {
        iters.push(SSTableIterator::new(table).await);
    }

    iters
}

pub async fn sstable_background(
    mut receiver: Receiver<CompactionSignal>,
    sstable_cache: SSTableCache,
    immutable: ImmutableMemtable,
    sstable_ack: Sender<SSTableLoadAck>,
) {
    while let Some(signal) = receiver.recv().await {
        match signal {
            CompactionSignal::LoadSSTable(path) => {
                /*
                 * Inser table.
                 * If needed, run compaction.
                 * Swap in new SSTable.
                 * */
                let sstable = SSTable::from_path(path).expect("Unable to read in SSTable file");
                let current_len = sstable_cache.push(sstable).await;
                immutable.pop().await;
                if current_len >= sstable_cache.compaction_rate {
                    let tables = sstable_cache.clone_tables().await;
                    let table_iters = tables_to_iterators(tables).await;
                    let (compact_table, compact_table_path) =
                        tokio::task::spawn_blocking(move || run_compaction(table_iters))
                            .await
                            .expect("Unable to run compaction");
                    cleanup(sstable_cache.clone(), compact_table, compact_table_path).await;
                }
                sstable_ack
                    .send(SSTableLoadAck::Done)
                    .await
                    .expect("Unable to send ack to signal SSTable is loaded");
            }
            CompactionSignal::Shutdown => return,
        }
    }
}

async fn cleanup(sstable_cache: SSTableCache, sstable: SSTable, new_table_path: PathBuf) {
    sstable_cache.replace_with_compact_table(sstable).await;

    let dir = read_dir(Path::new(".")).expect("Unable to read pwd to remove stale SSTable files.");

    for entry in dir {
        let entry = entry.expect("Unable to get directory entry");

        let path = entry.path();

        if new_table_path == path {
            continue;
        }

        if !path.is_file() {
            continue;
        }
        let Some(path_ext) = path.extension() else {
            continue;
        };

        if path_ext == "sstable" {
            remove_file(&path).expect("Unable to delete stale SSTable");
        }
    }
}

/*
* Generate iterators for all tables currently cached.
*
* Seed heap with one record per table.
*
* while heap not empty:
*   pop the smallest node
*
*   clear any nodes from the heap that have the same key
*   cleared nodes are replaced with the next node from the same table
*
*   smallest node is written to compaction writer
*
*   next node is taken from the iterator for the flushed node.
*
* Finish writing SSTable file.
* Load next in memory SSTable.
* Replace all cache SSTables with the compact SSTable.
* Remove all stale SSTable disk files.
* */
pub(crate) fn run_compaction(mut table_iters: Vec<SSTableIterator>) -> (SSTable, PathBuf) {
    let mut writer =
        CompactionWriter::new().expect("Unable to open SSTable file for CompactionWriter");
    let mut heap: BinaryHeap<Reverse<HeapNode>> = BinaryHeap::new();

    // Initially hydrate the heap with at most 1 node per SSTable file.
    for (i, sstable) in table_iters.iter_mut().enumerate() {
        if let Some(record) = sstable.next() {
            let heap_node = HeapNode::new(record, i);
            heap.push(Reverse(heap_node));
        }
    }

    while !heap.is_empty() {
        let Some(Reverse(smallest)) = heap.pop() else {
            break;
        };

        let smallest_key = &smallest.record.key;

        while let Some(record_ref) = heap.peek() {
            let inner_ref = &record_ref.0;
            if &inner_ref.record.key != smallest_key {
                break;
            }
            let table_idx = inner_ref.table_idx;

            heap.pop().unwrap();
            if let Some(record) = table_iters[table_idx].next() {
                let heap_node = HeapNode::new(record, table_idx);
                heap.push(Reverse(heap_node));
            }
        }

        let HeapNode {
            record: disk_record,
            table_idx,
        } = smallest;

        if matches!(disk_record.data, NodeData::Data(_)) {
            let _ = writer.push(disk_record);
        }

        if let Some(record) = table_iters[table_idx].next() {
            heap.push(Reverse(HeapNode::new(record, table_idx)));
        }
    }

    let new_table_path = writer.finish().expect("Unable to flush compaction writer");
    let sstable = SSTable::from_path(new_table_path.clone())
        .expect("Unable to create in memory SSTable representation");

    (sstable, new_table_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::inner::{MemtableInner, NodeData};
    use crate::sstable::SSTable;
    use crate::sstable::iterator::SSTableIterator;

    struct TempFile(PathBuf);
    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn unique_test_path() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("test_compaction_{id}.sstable").into()
    }

    async fn make_sstable(entries: &[(&[u8], NodeData)]) -> (SSTable, TempFile) {
        let (mut inner, wal_path) = MemtableInner::new_for_test();
        for (key, data) in entries {
            inner.insert(key.to_vec(), data.clone()).unwrap();
        }
        let path = unique_test_path();
        let mut fd = std::fs::File::create(&path).unwrap();
        inner.flush_to_disk(&mut fd).unwrap();
        drop(fd);
        let _ = std::fs::remove_file(&wal_path);
        let sstable = SSTable::from_path(path.clone()).unwrap();
        (sstable, TempFile(path))
    }

    async fn to_iter(table: SSTable) -> SSTableIterator {
        SSTableIterator::new(Arc::new(Mutex::new(table))).await
    }

    #[tokio::test]
    async fn test_disjoint_keys_all_present() {
        let (t0, _g0) = make_sstable(&[
            (b"a", NodeData::Data(b"va".to_vec())),
            (b"c", NodeData::Data(b"vc".to_vec())),
        ])
        .await;
        let (t1, _g1) = make_sstable(&[
            (b"b", NodeData::Data(b"vb".to_vec())),
            (b"d", NodeData::Data(b"vd".to_vec())),
        ])
        .await;

        let iters = vec![to_iter(t0).await, to_iter(t1).await];
        let (mut compact, compact_path) = run_compaction(iters);
        let _gc = TempFile(compact_path);

        assert!(compact.search(b"a").is_ok());
        assert!(compact.search(b"b").is_ok());
        assert!(compact.search(b"c").is_ok());
        assert!(compact.search(b"d").is_ok());
    }

    #[tokio::test]
    async fn test_newer_table_wins_on_duplicate_key() {
        // index 0 = newer (matches SSTableCache push_front ordering)
        let (newer, _g0) = make_sstable(&[(b"k", NodeData::Data(b"new_val".to_vec()))]).await;
        let (older, _g1) = make_sstable(&[(b"k", NodeData::Data(b"old_val".to_vec()))]).await;

        let iters = vec![to_iter(newer).await, to_iter(older).await];
        let (mut compact, compact_path) = run_compaction(iters);
        let _gc = TempFile(compact_path);

        assert_eq!(compact.search(b"k").unwrap(), b"new_val".to_vec());
    }

    #[tokio::test]
    async fn test_tombstone_dropped_from_output() {
        let (t0, _g0) = make_sstable(&[(b"k", NodeData::Tombstone)]).await;

        let iters = vec![to_iter(t0).await];
        let (mut compact, compact_path) = run_compaction(iters);
        let _gc = TempFile(compact_path);

        assert!(compact.search(b"k").is_err());
    }

    #[tokio::test]
    async fn test_tombstone_hides_older_data() {
        // Newer table (idx 0) has tombstone, older has data — key must not appear in output.
        let (newer, _g0) = make_sstable(&[(b"k", NodeData::Tombstone)]).await;
        let (older, _g1) = make_sstable(&[(b"k", NodeData::Data(b"stale".to_vec()))]).await;

        let iters = vec![to_iter(newer).await, to_iter(older).await];
        let (mut compact, compact_path) = run_compaction(iters);
        let _gc = TempFile(compact_path);

        assert!(compact.search(b"k").is_err());
    }

    #[tokio::test]
    async fn test_output_keys_are_sorted() {
        let (t0, _g0) = make_sstable(&[
            (b"z", NodeData::Data(b"vz".to_vec())),
            (b"a", NodeData::Data(b"va".to_vec())),
        ])
        .await;

        let iters = vec![to_iter(t0).await];
        let (mut compact, compact_path) = run_compaction(iters);
        let _gc = TempFile(compact_path);

        // Both keys accessible — SSTable search validates index ordering internally
        assert_eq!(compact.search(b"a").unwrap(), b"va".to_vec());
        assert_eq!(compact.search(b"z").unwrap(), b"vz".to_vec());
    }
}
