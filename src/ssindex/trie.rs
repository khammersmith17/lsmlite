use crate::memtable::inner::Blob;
use crate::util;

const WORD_END_FLAG: u64 = 1_u64 << 63;
const OFFSET_MASK: u64 = !(1_u64 << 63);
const UNMARKED_FLAG: u64 = 0;

// 1 for byte + 4 for u32 index.
const SIZE_PER_CHILD_NODE: usize = 5_usize;
// 1 byte for the size, 8 bytes for the offset.
const TRIE_NODE_HEADER_SIZE: usize = 9_usize;
// 4 bytes for the node count.
const TRIE_HEADER_SIZE: usize = 4_usize;

// byte for char and index into arena.
type TriePointer = (u8, u32);

/*
* TrieNode serialization.
*
* [1 byte child size] [8 byte node offset value] [child buffer]
*
* child buffer will be n_child * [u8 char][4 byte be u32 index]
*
* which yields
*
* [u8][u64][n_child * [u8][u32]] per node.
* */
#[derive(Debug)]
struct TrieNode {
    // The MSB marks the end of a word here. This limits the size of the SSTable file to
    // 2^63 - 1, which is a reasonable limit here.
    offset: u64,
    children: Vec<TriePointer>,
}

impl Default for TrieNode {
    fn default() -> TrieNode {
        // TODO: defaults to 4, figure out better way to be able to configure this.
        let children: Vec<TriePointer> = Vec::with_capacity(4);
        TrieNode {
            offset: UNMARKED_FLAG,
            children,
        }
    }
}

impl TrieNode {
    fn deserialize_from_disk(buffer: &[u8], buffer_offset: &mut usize) -> TrieNode {
        let num_children = buffer[*buffer_offset];
        *buffer_offset += 1;
        let mut children: Vec<TriePointer> = Vec::with_capacity(usize::from(num_children));

        let offset = u64::from_be_bytes(util::get_be_array8(buffer, *buffer_offset));
        *buffer_offset += 8;

        for _ in 0..num_children {
            let c = buffer[*buffer_offset];
            *buffer_offset += 1;

            let arena_idx = u32::from_be_bytes(util::get_be_array4(buffer, *buffer_offset));
            *buffer_offset += 4;

            children.push((c, arena_idx));
        }

        TrieNode { offset, children }
    }

    fn serialized_size(&self) -> usize {
        TRIE_NODE_HEADER_SIZE + (SIZE_PER_CHILD_NODE * self.children.len())
    }

    fn serialize_into_buffer(&self, buffer: &mut Vec<u8>, offset: &mut usize) {
        buffer[*offset] = self.children.len() as u8;
        *offset += 1;
        buffer[*offset..*offset + 8].copy_from_slice(&self.offset.to_be_bytes());
        *offset += 8;

        for child in &self.children {
            buffer[*offset] = child.0;
            *offset += 1;
            buffer[*offset..*offset + 4].copy_from_slice(&child.1.to_be_bytes());
            *offset += 4;
        }
    }

    fn set_offset(&mut self, offset: u64) {
        self.offset = offset | WORD_END_FLAG;
    }

    fn get_offset(&self) -> Option<u64> {
        if self.offset == UNMARKED_FLAG {
            return None;
        }
        Some(self.offset & OFFSET_MASK)
    }

    // Returns the index of the child in the arena if the child is present, otherwise returns the
    // insertion index of the child in the nodes child buffer.
    fn get_child_idx(&self, c: u8) -> Result<usize, usize> {
        let child_idx = self.children.binary_search_by(|child| child.0.cmp(&c))?;
        Ok(self.children[child_idx].1 as usize)
    }

    // Linear insert will be fast as this buffer is at most 256 large.
    fn insert_child(&mut self, pos: usize, c: u8, idx: u32) {
        self.children.insert(pos, (c, idx))
    }
}

/*
* Serializing Trie.
*
* The arena will be serialized flat, using the per node serialization protocol.
* The trie will have a header detailing the number of nodes in the arena, and the total size of the
* buffer that needs to be read to pull in the entire trie.
* [number of nodes]
* [u32]
* */

/// This trie implementation is a flat arena, with node entries that logically represent the tree.
/// The flat arena is intended to improve cache locality, preventing trie nodes from being
/// scattered across the heap, and to improve space. Each node likely needs ~4 pointers per node.
#[derive(Default, Debug)]
pub(crate) struct Trie {
    arena: Vec<TrieNode>, // root node is first.
}

impl Trie {
    // Capacity should be the number of keys in the SSTable file. This works well when many keys
    // share a same prefix.
    //
    // This works less well when key prefixes are rather disjoint. In this case there might be a
    // reallocation in the arena.
    //
    // This is just a heuristic and does not provide a strong prediction.
    //
    // When reading the trie from disk, we will know the exact capacity.
    pub(crate) fn new(capacity: usize) -> Trie {
        let arena = Vec::with_capacity(capacity);

        Trie { arena }
    }

    // Expects the ssindex tighlty packed in the buffer.
    pub(crate) fn deserialize_from_disk(buffer: &[u8]) -> Trie {
        let mut buffer_offset = 0_usize;
        let arena_size = u32::from_be_bytes(util::get_be_array4(buffer, buffer_offset)) as usize;

        let mut arena: Vec<TrieNode> = Vec::with_capacity(arena_size);

        buffer_offset += 4;

        for _ in 0..arena_size {
            let node = TrieNode::deserialize_from_disk(buffer, &mut buffer_offset);
            arena.push(node);
        }

        Trie { arena }
    }

    pub(crate) fn serialize(self) -> Blob {
        let total_size = self.serialized_size();
        let mut buffer = util::make_blob_buffer(total_size);
        let mut offset = 0_usize;

        // Write node count header.
        buffer[offset..offset + 4].copy_from_slice(&(self.arena.len() as u32).to_be_bytes());
        offset += 4;

        // Write all nodes directly into the buffer.
        for node in &self.arena {
            node.serialize_into_buffer(&mut buffer, &mut offset);
        }

        buffer
    }

    fn serialized_size(&self) -> usize {
        TRIE_HEADER_SIZE
            + self
                .arena
                .iter()
                .map(|n| n.serialized_size())
                .sum::<usize>()
    }

    pub(crate) fn insert(&mut self, key: &[u8], offset: u64) {
        if self.arena.is_empty() {
            self.arena.push(TrieNode::default());
        }

        let mut arena_idx = 0_usize;

        for c in key {
            arena_idx = self.get_or_insert_node(arena_idx, *c);
        }

        self.arena[arena_idx].set_offset(offset);
    }

    // When traversing to the child of a given node, this handles the case where that node has yet
    // to be created.
    //
    // If the node exists, the index is returned, otherwise the node is inserted into the arena,
    // and then added as a child to the current node pointer.
    fn get_or_insert_node(&mut self, arena_idx: usize, c: u8) -> usize {
        match self.arena[arena_idx].get_child_idx(c) {
            Ok(child_idx) => child_idx,
            Err(child_insert) => {
                let child_idx = self.arena.len();
                self.arena.push(TrieNode::default());
                self.arena[arena_idx].insert_child(child_insert, c, child_idx as u32);
                child_idx
            }
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<u64> {
        if self.arena.is_empty() {
            return None;
        };
        let mut arena_idx = 0_usize;

        for c in key {
            let Ok(idx) = self.arena[arena_idx].get_child_idx(*c) else {
                return None;
            };
            arena_idx = idx;
        }

        self.arena[arena_idx].get_offset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trie(entries: &[(&[u8], u64)]) -> Trie {
        let mut trie = Trie::new(entries.len());
        for (key, offset) in entries {
            trie.insert(key, *offset);
        }
        trie
    }

    fn roundtrip(trie: Trie) -> Trie {
        let blob = trie.serialize();
        Trie::deserialize_from_disk(&blob)
    }

    #[test]
    fn empty_trie_returns_none() {
        let trie = Trie::new(0);
        assert_eq!(trie.get(b"key"), None);
    }

    #[test]
    fn single_key_insert_and_get() {
        let trie = make_trie(&[(b"hello", 42)]);
        assert_eq!(trie.get(b"hello"), Some(42));
    }

    #[test]
    fn missing_key_returns_none() {
        let trie = make_trie(&[(b"hello", 42)]);
        assert_eq!(trie.get(b"world"), None);
    }

    #[test]
    fn prefix_of_key_is_not_terminal() {
        // "hell" is a prefix of "hello" - must not be found.
        let trie = make_trie(&[(b"hello", 42)]);
        assert_eq!(trie.get(b"hell"), None);
    }

    #[test]
    fn extension_beyond_key_returns_none() {
        let trie = make_trie(&[(b"hello", 42)]);
        assert_eq!(trie.get(b"helloworld"), None);
    }

    #[test]
    fn multiple_disjoint_keys() {
        let trie = make_trie(&[(b"apple", 1), (b"banana", 2), (b"cherry", 3)]);
        assert_eq!(trie.get(b"apple"), Some(1));
        assert_eq!(trie.get(b"banana"), Some(2));
        assert_eq!(trie.get(b"cherry"), Some(3));
        assert_eq!(trie.get(b"appl"), None);
    }

    #[test]
    fn shared_prefix_keys() {
        let trie = make_trie(&[(b"foo", 10), (b"foobar", 20), (b"foobaz", 30)]);
        assert_eq!(trie.get(b"foo"), Some(10));
        assert_eq!(trie.get(b"foobar"), Some(20));
        assert_eq!(trie.get(b"foobaz"), Some(30));
        assert_eq!(trie.get(b"fooba"), None);
    }

    #[test]
    fn key_is_prefix_of_another_both_inserted() {
        let trie = make_trie(&[(b"ab", 5), (b"abc", 6)]);
        assert_eq!(trie.get(b"ab"), Some(5));
        assert_eq!(trie.get(b"abc"), Some(6));
        assert_eq!(trie.get(b"a"), None);
        assert_eq!(trie.get(b"abcd"), None);
    }

    #[test]
    fn insert_out_of_order_keys() {
        // Children must remain sorted regardless of insertion order.
        let trie = make_trie(&[(b"z", 100), (b"a", 1), (b"m", 50)]);
        assert_eq!(trie.get(b"z"), Some(100));
        assert_eq!(trie.get(b"a"), Some(1));
        assert_eq!(trie.get(b"m"), Some(50));
    }

    #[test]
    fn offset_zero_is_valid() {
        let trie = make_trie(&[(b"key", 0)]);
        assert_eq!(trie.get(b"key"), Some(0));
    }

    #[test]
    fn max_offset_below_msb() {
        // Max valid offset is 2^63 - 1 (MSB reserved for WORD_END_FLAG).
        let max_offset = (1_u64 << 63) - 1;
        let trie = make_trie(&[(b"key", max_offset)]);
        assert_eq!(trie.get(b"key"), Some(max_offset));
    }

    #[test]
    fn roundtrip_empty_trie() {
        let trie = Trie::new(0);
        let restored = roundtrip(trie);
        assert_eq!(restored.get(b"anything"), None);
    }

    #[test]
    fn roundtrip_single_key() {
        let trie = make_trie(&[(b"hello", 42)]);
        let restored = roundtrip(trie);
        assert_eq!(restored.get(b"hello"), Some(42));
        assert_eq!(restored.get(b"world"), None);
        assert_eq!(restored.get(b"hell"), None);
    }

    #[test]
    fn roundtrip_multiple_disjoint_keys() {
        let trie = make_trie(&[(b"apple", 1), (b"banana", 2), (b"cherry", 3)]);
        let restored = roundtrip(trie);
        assert_eq!(restored.get(b"apple"), Some(1));
        assert_eq!(restored.get(b"banana"), Some(2));
        assert_eq!(restored.get(b"cherry"), Some(3));
    }

    #[test]
    fn roundtrip_shared_prefix_keys() {
        let trie = make_trie(&[(b"foo", 10), (b"foobar", 20), (b"foobaz", 30)]);
        let restored = roundtrip(trie);
        assert_eq!(restored.get(b"foo"), Some(10));
        assert_eq!(restored.get(b"foobar"), Some(20));
        assert_eq!(restored.get(b"foobaz"), Some(30));
        assert_eq!(restored.get(b"fooba"), None);
    }

    #[test]
    fn roundtrip_prefix_and_extension_both_inserted() {
        let trie = make_trie(&[(b"ab", 5), (b"abc", 6)]);
        let restored = roundtrip(trie);
        assert_eq!(restored.get(b"ab"), Some(5));
        assert_eq!(restored.get(b"abc"), Some(6));
        assert_eq!(restored.get(b"a"), None);
    }

    #[test]
    fn roundtrip_out_of_order_keys() {
        let trie = make_trie(&[(b"z", 100), (b"a", 1), (b"m", 50)]);
        let restored = roundtrip(trie);
        assert_eq!(restored.get(b"z"), Some(100));
        assert_eq!(restored.get(b"a"), Some(1));
        assert_eq!(restored.get(b"m"), Some(50));
    }
}
