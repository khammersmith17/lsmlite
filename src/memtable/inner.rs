use super::record::{Record, RecordType};
use crate::compaction::{CompactionSignal, SSTableLoadAck};
use crate::error::MemtableError;
use crate::memtable::immutable::ImmutableMemtable;
use crate::sstable;
use crate::util;
use crate::wal::Wal;
use crate::wal::WalIterator;
use std::cmp::Ordering;
use std::fs::File;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{self, AtomicBool},
};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, PartialOrd, PartialEq)]
pub enum MemtableQuery {
    Data(Blob),
    Tombstone,
    None,
}

pub enum MemtableFlushSignal {
    Flush,
    Shutdown,
}

/*
  1. Every node is either red or black
  2. The root is black
  3. Every NIL leaf is black
  4. If a node is red, both its children are black
  5. All paths from any node to its descendant NIL leaves contain the same number of black nodes (uniform black-height)

  Plus the BST property: left subtree keys < node key < right subtree keys.

  Rotations (the core structural operation):
  - Left rotate on N: N's right child takes N's place, N becomes its left child
  - Right rotate on N: N's left child takes N's place, N becomes its right child
  - Rotations preserve BST order

  Insert fixup (after inserting a red node):
  - Case 1: Uncle is red → recolor parent + uncle black, grandparent red, move up
  - Case 2: Uncle is black, node is inner child → rotate parent, becomes Case 3
  - Case 3: Uncle is black, node is outer child → rotate grandparent, swap colors of parent/grandparent

  Delete fixup (after removing a black node, propagate "double black"):
  - Case 1: Sibling is red → rotate parent, reduces to Cases 2–4
  - Case 2: Sibling is black, both sibling's children black → recolor sibling red, move double-black up
  - Case 3: Sibling is black, near child red, far child black → rotate sibling, reduces to Case 4
  - Case 4: Sibling is black, far child red → rotate parent, done
* */

// Flush a full memtable and send signal to background compaction thread to read in the new SSTable
// file and push it onto the SSTable cache.
pub async fn flush_memtable(
    immutable: ImmutableMemtable,
    signal: Sender<CompactionSignal>,
    poisoned_flag: Arc<AtomicBool>,
    mut flush_recv: Receiver<MemtableFlushSignal>,
    mut sstable_load_ack: Receiver<SSTableLoadAck>,
) {
    while let Some(memtable_signal) = flush_recv.recv().await {
        if matches!(memtable_signal, MemtableFlushSignal::Shutdown) {
            return;
        }

        let pathname = util::generate_sstable_file_name();
        let Ok(mut fd) = File::create(&pathname) else {
            poisoned_flag.swap(true, atomic::Ordering::Release);
            return;
        };

        match immutable.flush(&mut fd).await {
            Ok(_) => {
                let _ = signal.send(CompactionSignal::LoadSSTable(pathname)).await;
                // Wait for Ack in the case that multiple immutable tables are queued for flush.
                // This is to avoid race conditions of attempting to flush an immutable memtable
                // while the previous SSTable index is not yet read into memory for reads.
                let _ = sstable_load_ack
                    .recv()
                    .await
                    .expect("Unable to receive ack loaded SSTable");
            }
            Err(_) => {
                poisoned_flag.swap(true, atomic::Ordering::Release);
            }
        }
    }
}

pub fn flush_memtable_inner(fd: &mut File, memtable: Arc<MemtableInner>) -> std::io::Result<()> {
    memtable.flush_to_disk(fd)?;
    let path = memtable.wal.filename.clone();
    std::fs::remove_file(path)
}

// Determine inner vs outer child.
#[inline]
fn child_type(
    grandparent_left: Option<usize>,
    parent: usize,
    parent_left: Option<usize>,
    node: usize,
) -> ChildType {
    let is_parent_left = grandparent_left == Some(parent);
    let is_node_left = parent_left == Some(node);

    if is_parent_left == is_node_left {
        ChildType::Outer
    } else {
        ChildType::Inner
    }
}

fn derive_inner_rotation(grandparent_left: Option<usize>, parent: usize) -> RotationDirection {
    // Left right zig zag left rotate parent.
    // Otherwise right rotate parent.
    if grandparent_left == Some(parent) {
        // Left-Right zig zag.
        RotationDirection::Left
    } else {
        RotationDirection::Right
    }
}

fn derive_outer_rotation(grandparent_left: Option<usize>, parent: usize) -> RotationDirection {
    // Left - Left right rotate.
    if grandparent_left == Some(parent) {
        RotationDirection::Right
    } else {
        RotationDirection::Left
    }
}

// Determine inner vs outer child and the rotation direction.
fn derive_rotation_direction(
    grandparent_left: Option<usize>,
    parent: usize,
    parent_left: Option<usize>,
    node: usize,
) -> (ChildType, RotationDirection) {
    match child_type(grandparent_left, parent, parent_left, node) {
        ChildType::Outer => (
            ChildType::Outer,
            derive_outer_rotation(grandparent_left, parent),
        ),
        ChildType::Inner => (
            ChildType::Inner,
            derive_inner_rotation(grandparent_left, parent),
        ),
    }
}

pub type Blob = Vec<u8>;

enum ChildType {
    Inner,
    Outer,
}

enum RotationDirection {
    Left,
    Right,
}

enum Child {
    Left,
    Right,
}

enum InsertionPosition {
    NewNode(usize, Child), // Parent index.
    ExistingNode(usize),   // Index of node to mutate.
}

#[derive(Debug, PartialEq, Eq)]
enum Color {
    Red,
    Black,
}

#[derive(Debug, Eq)]
pub struct MemtableNode {
    record: Record,
    pub left: Option<usize>,
    pub right: Option<usize>,
    parent: Option<usize>,
    color: Color,
}

impl PartialEq for MemtableNode {
    fn eq(&self, other: &MemtableNode) -> bool {
        self.key() == other.key()
    }
}

impl PartialOrd for MemtableNode {
    fn partial_cmp(&self, other: &MemtableNode) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MemtableNode {
    fn cmp(&self, other: &MemtableNode) -> Ordering {
        self.key().cmp(&other.key())
    }
}

impl MemtableNode {
    pub(crate) fn copy_record(&self) -> Blob {
        self.record.copy_record()
    }

    fn record_type(&self) -> RecordType {
        self.record.record_type()
    }

    fn copy_data(&self) -> Option<Blob> {
        self.record.copy_value()
    }

    fn size(&self) -> usize {
        self.record.size()
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.record.key()
    }
    // Returns the index of a nodes uncle, ie the other Node that descends from grandparent in the
    // tree.
    fn uncle(&self, arena: &[MemtableNode]) -> Option<usize> {
        let parent = &arena[self.parent?];
        let grandparent = &arena[parent.parent?];

        // If parent index is grandparents left, uncle is right, otherwise uncle is left.
        if grandparent.left == self.parent {
            grandparent.right
        } else {
            grandparent.left
        }
    }

    fn update_data_from_node(&mut self, node: MemtableNode) {
        self.record.update_from_record(&node.record);
    }

    fn new_data_record(key: Blob, value: Blob) -> MemtableNode {
        let record = Record::new_data_record(key, value);
        MemtableNode {
            record,
            left: None,
            right: None,
            parent: None,
            color: Color::Red,
        }
    }

    fn new_from_record(record: Record) -> MemtableNode {
        MemtableNode {
            record,
            left: None,
            right: None,
            parent: None,
            color: Color::Red,
        }
    }

    fn new_tombstone_record(key: Blob) -> MemtableNode {
        let record = Record::new_tombstone_record(key);
        MemtableNode {
            record,
            left: None,
            right: None,
            parent: None,
            color: Color::Red,
        }
    }
}

#[derive(Debug)]
pub struct MemtableInner {
    pub arena: Vec<MemtableNode>,
    pub max_size: usize,
    pub root_node: Option<usize>,
    pub current_size: usize,
    pub wal: Wal,
}

impl MemtableInner {
    pub fn new(max_size: usize, max_nodes: usize) -> Result<MemtableInner, std::io::Error> {
        let arena = Vec::with_capacity(max_nodes);
        let wal = Wal::new()?;

        Ok(MemtableInner {
            arena,
            max_size,
            root_node: None,
            current_size: 0_usize,
            wal,
        })
    }

    pub fn from_wal(
        wal_filepath: &Path,
        max_size: usize,
        max_nodes: usize,
    ) -> std::io::Result<MemtableInner> {
        let wal_iter = WalIterator::new(wal_filepath)?;
        let mut table = Self::new(max_size, max_nodes)?;
        for record in wal_iter {
            // SAFETY: Recovering from WAL should not exceed max memtable size.
            let _ = table.insert_record_from_wal(record);
        }

        std::fs::remove_file(wal_filepath)?;
        Ok(table)
    }

    fn full(&self) -> bool {
        self.arena.len() == self.arena.capacity() || self.current_size >= self.max_size
    }

    pub fn flush_to_disk(&self, fd: &mut File) -> std::io::Result<()> {
        sstable::encode::write_sstable(self, fd)?;
        Ok(())
    }

    // If full -> flush.
    // Return the write request back out to the caller. The caller rotates the table, and then
    // calls again. The next write will then succeed.
    pub fn insert_data_record(&mut self, key: Blob, value: Blob) -> Result<(), MemtableError> {
        if self.full() {
            return Err(MemtableError::TableFull(key, Some(value)));
        }

        let node = MemtableNode::new_data_record(key, value);
        self.wal.write_log(&node);

        // Insert node into tree.
        self.insert_node(node);
        Ok(())
    }

    pub fn insert_tombstone_record(&mut self, key: Blob) -> Result<(), MemtableError> {
        if self.full() {
            return Err(MemtableError::TableFull(key, None));
        }

        let node = MemtableNode::new_tombstone_record(key);
        self.wal.write_log(&node);

        // Insert node into tree.
        self.insert_node(node);
        Ok(())
    }

    pub fn insert_record_from_wal(&mut self, record: Record) -> Result<(), MemtableError> {
        if self.full() {
            todo!(
                "Implement the case where backfilling from wal yields full table.
                Consider when the engine is started from disk state and the configs are changed"
            );
        }
        let node = MemtableNode::new_from_record(record);
        self.insert_node(node);
        Ok(())
    }

    fn insert_node(&mut self, mut node: MemtableNode) {
        /*
         * Insertion:
         *   1. Check root existance
         *       If not root, set and return
         *   2. Find insert position in the tree
         *       If a node with the same value exists in the tree, update that node
         *       and return
         *   3. With the parent and left/right child
         *       give the new node the parent pointer
         *       update the parent with the new nodes pointer
         *    4. Push the node into the buffer
         *    5. Resolve Red black tree structure
         * */
        // The nodes destination index in the buffer.
        let node_idx = self.arena.len();

        // Tree is empty, insert at root and change color.
        if self.root_node.is_none() {
            self.set_root(node, node_idx);
            return;
        }

        let (parent_ptr, child) = match self.find_insert_position(&node) {
            InsertionPosition::ExistingNode(curr_idx) => {
                self.arena[curr_idx].update_data_from_node(node);
                return;
            }
            InsertionPosition::NewNode(parent, child) => (parent, child),
        };

        let parent_node = &mut self.arena[parent_ptr];
        match child {
            Child::Left => parent_node.left = Some(node_idx),
            Child::Right => parent_node.right = Some(node_idx),
        }

        node.parent = Some(parent_ptr);
        self.current_size += node.size();
        self.arena.push(node);
        self.insert_fix_tree(node_idx);
    }

    fn set_root(&mut self, mut node: MemtableNode, node_idx: usize) {
        self.root_node = Some(node_idx); // Should be 0.
        node.color = Color::Black;
        self.arena.push(node);
    }

    fn find_insert_position(&self, key: &MemtableNode) -> InsertionPosition {
        let mut curr = self.root_node.unwrap();

        #[allow(unused_assignments)]
        let mut parent = curr;

        loop {
            parent = curr;
            let node = &self.arena[curr];
            match key.cmp(node) {
                Ordering::Equal => {
                    return InsertionPosition::ExistingNode(curr);
                }
                Ordering::Less => {
                    if let Some(child_idx) = node.left {
                        curr = child_idx;
                    } else {
                        return InsertionPosition::NewNode(parent, Child::Left);
                    }
                }
                Ordering::Greater => {
                    if let Some(child_idx) = node.right {
                        curr = child_idx;
                    } else {
                        return InsertionPosition::NewNode(parent, Child::Right);
                    }
                }
            }
        }
    }

    fn insert_fix_tree(&mut self, mut node_idx: usize) {
        loop {
            // Case 1
            let Some(parent_idx) = self.arena[node_idx].parent else {
                break;
            };

            if self.arena[parent_idx].color == Color::Black {
                break;
            }

            // SAFETY: Grandparent exists as parent is Red, thus not root.
            let grandparent_idx = self.arena[parent_idx].parent.unwrap();
            match (self.arena[node_idx]).uncle(&self.arena) {
                Some(u) if self.arena[u].color == Color::Red => {
                    self.arena[u].color = Color::Black;
                    self.arena[parent_idx].color = Color::Black;
                    self.arena[grandparent_idx].color = Color::Red;
                    node_idx = grandparent_idx;
                }
                _ => {
                    // Uncle is black.
                    match derive_rotation_direction(
                        self.arena[grandparent_idx].left,
                        parent_idx,
                        self.arena[parent_idx].left,
                        node_idx,
                    ) {
                        // Case 2: Rotate parent.
                        (ChildType::Inner, RotationDirection::Right) => {
                            self.right_rotate(parent_idx);
                            node_idx = parent_idx;
                        }
                        (ChildType::Inner, RotationDirection::Left) => {
                            self.left_rotate(parent_idx);
                            node_idx = parent_idx;
                        }
                        // Case 3: Rotate grandparent.
                        (ChildType::Outer, RotationDirection::Right) => {
                            self.insert_recolor(parent_idx, grandparent_idx);
                            self.right_rotate(grandparent_idx);
                            break;
                        }
                        (ChildType::Outer, RotationDirection::Left) => {
                            self.insert_recolor(parent_idx, grandparent_idx);
                            self.left_rotate(grandparent_idx);
                            break;
                        }
                    }
                }
            }
        }

        // Rule 2: root is always black.
        self.arena[self.root_node.unwrap()].color = Color::Black;
    }

    fn insert_recolor(&mut self, parent: usize, grandparent: usize) {
        self.arena[parent].color = Color::Black;
        self.arena[grandparent].color = Color::Red;
    }

    fn left_rotate(&mut self, n: usize) {
        let r = self.arena[n]
            .right
            .expect("Right child needs to exists for left rotation");

        // R's left child becomes N's right child.
        self.arena[n].right = self.arena[r].left;
        if let Some(r_left) = self.arena[r].left {
            self.arena[r_left].parent = Some(n)
        };

        // Swap R and N.
        self.arena[r].parent = self.arena[n].parent;
        match self.arena[n].parent {
            Some(p) => {
                if self.arena[p].left == Some(n) {
                    self.arena[p].left = Some(r)
                } else {
                    self.arena[p].right = Some(r)
                }
            }
            None => self.root_node = Some(r),
        }

        self.arena[r].left = Some(n);
        self.arena[n].parent = Some(r);
    }

    fn right_rotate(&mut self, n: usize) {
        let l = self.arena[n]
            .left
            .expect("Left child needs to exists for right rotations");

        // L's right child becomes N's left child.
        self.arena[n].left = self.arena[l].right;
        if let Some(l_right) = self.arena[l].right {
            self.arena[l_right].parent = Some(n)
        }

        // Swap L and N.
        self.arena[l].parent = self.arena[n].parent;
        match self.arena[n].parent {
            Some(p) => {
                if self.arena[p].left == Some(n) {
                    self.arena[p].left = Some(l)
                } else {
                    self.arena[p].right = Some(l)
                }
            }
            None => self.root_node = Some(l),
        }
        self.arena[l].right = Some(n);
        self.arena[n].parent = Some(l);
    }

    // Always returned owned copy of the data segment.
    pub fn get(&self, key: &[u8]) -> MemtableQuery {
        let Some(node_idx) = self.get_search(key) else {
            return MemtableQuery::None;
        };
        match self.arena[node_idx].record_type() {
            // SAFETY: Record type invarints dictate that the record is not a tombstone, thus has
            // data to be copied.
            RecordType::Data => MemtableQuery::Data(self.arena[node_idx].copy_data().unwrap()),
            RecordType::Tombstone => MemtableQuery::Tombstone,
        }
    }

    fn get_search(&self, key: &[u8]) -> Option<usize> {
        let mut curr = self.root_node?;

        loop {
            let curr_node = &self.arena[curr];
            match key.cmp(&curr_node.key()) {
                Ordering::Equal => return Some(curr),
                Ordering::Less => {
                    if let Some(left) = curr_node.left {
                        curr = left;
                    } else {
                        return None;
                    }
                }
                Ordering::Greater => {
                    if let Some(right) = curr_node.right {
                        curr = right
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
impl MemtableNode {
    pub fn new_for_test(key: Blob, data: Blob) -> MemtableNode {
        MemtableNode::new_data_record(key, data)
    }

    pub fn new_tombstone_for_test(key: Blob) -> MemtableNode {
        MemtableNode::new_tombstone_record(key)
    }
}

#[cfg(test)]
impl MemtableInner {
    pub fn new_for_test() -> (MemtableInner, std::path::PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let thread_id = std::thread::current().id();
        let wal_path: std::path::PathBuf = format!("test_wal_{thread_id:?}_{id}.log").into();
        let fd = std::fs::File::create(&wal_path).unwrap();
        let wal = Wal::from_fd_and_path(fd, wal_path.clone());
        let inner = MemtableInner {
            arena: Vec::with_capacity(64),
            max_size: usize::MAX,
            root_node: None,
            current_size: 0,
            wal,
        };
        (inner, wal_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    struct TestMemtable {
        inner: MemtableInner,
        wal_path: PathBuf,
    }

    impl Drop for TestMemtable {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.wal_path);
        }
    }

    impl std::ops::DerefMut for TestMemtable {
        fn deref_mut(&mut self) -> &mut MemtableInner {
            &mut self.inner
        }
    }

    impl std::ops::Deref for TestMemtable {
        type Target = MemtableInner;
        fn deref(&self) -> &MemtableInner {
            &self.inner
        }
    }

    fn make_memtable() -> TestMemtable {
        let thread_id = std::thread::current().id();
        let wal_path: PathBuf = format!("test_wal_{thread_id:?}.log").into();
        let fd = fs::File::create(&wal_path).unwrap();
        let wal = Wal::from_fd_and_path(fd, wal_path.clone());
        let inner = MemtableInner {
            arena: Vec::with_capacity(64),
            max_size: usize::MAX,
            root_node: None,
            current_size: 0,
            wal,
        };
        TestMemtable { inner, wal_path }
    }

    // Recursively checks RB invariants, returns the black-height of the subtree.
    fn check_subtree(arena: &[MemtableNode], node: Option<usize>) -> usize {
        let Some(idx) = node else {
            return 1; // NIL counts as black
        };
        let n = &arena[idx];
        if n.color == Color::Red {
            assert!(
                n.left.map_or(true, |l| arena[l].color == Color::Black),
                "Red node {idx} has red left child"
            );
            assert!(
                n.right.map_or(true, |r| arena[r].color == Color::Black),
                "Red node {idx} has red right child"
            );
        }
        let lbh = check_subtree(arena, n.left);
        let rbh = check_subtree(arena, n.right);
        assert_eq!(lbh, rbh, "Black height mismatch at node {idx}");
        if n.color == Color::Black {
            lbh + 1
        } else {
            lbh
        }
    }

    fn check_invariants(t: &MemtableInner) {
        let Some(root) = t.root_node else { return };
        assert_eq!(t.arena[root].color, Color::Black, "Root must be black");
        check_subtree(&t.arena, Some(root));
    }

    #[test]
    fn test_root_is_black() {
        let mut t = make_memtable();
        t.insert_data_record(b"m".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(t.arena[t.root_node.unwrap()].color, Color::Black);
    }

    #[test]
    fn test_get_after_insert() {
        let mut t = make_memtable();
        t.insert_data_record(b"hello".to_vec(), b"world".to_vec())
            .unwrap();
        assert_eq!(t.get(b"hello"), MemtableQuery::Data(b"world".to_vec()));
        assert_eq!(t.get(b"missing"), MemtableQuery::None);
    }

    #[test]
    fn test_duplicate_key_updates_value() {
        let mut t = make_memtable();
        t.insert_data_record(b"key".to_vec(), b"v1".to_vec())
            .unwrap();
        t.insert_data_record(b"key".to_vec(), b"v2".to_vec())
            .unwrap();
        assert_eq!(t.get(b"key"), MemtableQuery::Data(b"v2".to_vec()));
    }

    #[test]
    fn test_case1_uncle_red() {
        // m(B) → e(R) left, t(R) right, then a(R) left of e
        // parent e is red, uncle t is red → Case 1: recolor e,t black, m red → m forced black
        let mut t = make_memtable();
        for k in [b"m", b"e", b"t", b"a"] {
            t.insert_data_record(k.to_vec(), b"".to_vec()).unwrap();
        }
        check_invariants(&t);
        let root = t.root_node.unwrap();
        let e_idx = t.arena[root].left.unwrap();
        let t_idx = t.arena[root].right.unwrap();
        assert_eq!(t.arena[e_idx].color, Color::Black);
        assert_eq!(t.arena[t_idx].color, Color::Black);
    }

    #[test]
    fn test_case3_right_right() {
        // a → b → c: right-right straight line → left rotate a, b becomes root
        let mut t = make_memtable();
        for k in [b"a", b"b", b"c"] {
            t.insert_data_record(k.to_vec(), b"".to_vec()).unwrap();
        }
        check_invariants(&t);
        let root = t.root_node.unwrap();
        assert_eq!(t.arena[root].key(), b"b");
        assert_eq!(t.arena[root].color, Color::Black);
    }

    #[test]
    fn test_case3_left_left() {
        // c → b → a: left-left straight line → right rotate c, b becomes root
        let mut t = make_memtable();
        for k in [b"c", b"b", b"a"] {
            t.insert_data_record(k.to_vec(), b"".to_vec()).unwrap();
        }
        check_invariants(&t);
        let root = t.root_node.unwrap();
        assert_eq!(t.arena[root].key(), b"b");
        assert_eq!(t.arena[root].color, Color::Black);
    }

    #[test]
    fn test_case2_case3_right_left() {
        // a → c → b: right-left zigzag → Case 2 (right rotate c) then Case 3, b becomes root
        let mut t = make_memtable();
        for k in [b"a", b"c", b"b"] {
            t.insert_data_record(k.to_vec(), b"".to_vec()).unwrap();
        }
        check_invariants(&t);
        let root = t.root_node.unwrap();
        assert_eq!(t.arena[root].key(), b"b");
        assert_eq!(t.arena[root].color, Color::Black);
    }

    #[test]
    fn test_case2_case3_left_right() {
        // c → a → b: left-right zigzag → Case 2 (left rotate a) then Case 3, b becomes root
        let mut t = make_memtable();
        for k in [b"c", b"a", b"b"] {
            t.insert_data_record(k.to_vec(), b"".to_vec()).unwrap();
        }
        check_invariants(&t);
        let root = t.root_node.unwrap();
        assert_eq!(t.arena[root].key(), b"b");
        assert_eq!(t.arena[root].color, Color::Black);
    }

    #[test]
    fn test_invariants_many_inserts() {
        let mut t = make_memtable();
        for k in [b"f", b"b", b"g", b"a", b"d", b"i", b"c", b"e", b"h"] {
            t.insert_data_record(k.to_vec(), b"".to_vec()).unwrap();
            check_invariants(&t);
        }
    }

    #[test]
    fn test_all_keys_retrievable() {
        let mut t = make_memtable();
        let keys = [b"f", b"b", b"g", b"a", b"d", b"i", b"c", b"e", b"h"];
        for k in keys {
            t.insert_data_record(k.to_vec(), vec![*k.first().unwrap()])
                .unwrap();
        }
        for k in keys {
            assert_eq!(
                t.get(k),
                MemtableQuery::Data(vec![*k.first().unwrap()]),
                "Missing key {:?}",
                k
            );
        }
    }
}
