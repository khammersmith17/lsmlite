struct TrieNode {
    offset: Option<u64>,
    children: [Option<Box<TrieNode>>; 256],
}

impl Default for TrieNode {
    fn default() -> TrieNode {
        let children = [const { None }; 256];
        TrieNode {
            offset: None,
            children,
        }
    }
}

pub(super) struct Trie {
    head: TrieNode,
}

/*
*The algorithm:

  floor(trie, key) → Option<u64>:
    curr = root
    best_offset = root.offset  // if root has one

    for each byte b in key:
      if curr.children[b] exists:
        // Exact prefix match — keep descending
        curr = curr.children[b]
        if curr.offset is Some:
          best_offset = curr.offset
      else:
        // No exact child — find the largest child < b
        for c from b-1 down to 0:
          if curr.children[c] exists:
            // Descend into it and take the rightmost path to a leaf
            best_offset = rightmost(curr.children[c])
            break
        // Either way, stop descending
        break

    return best_offset

  rightmost(node) → u64:
    // Walk to the lexicographically largest leaf under this node
    curr = node
    last = curr.offset
    loop:
      find child with highest index
      if none: return last
      curr = that child
      if curr.offset is Some: last = curr.offset
* */

impl Trie {
    pub(super) fn new() -> Trie {
        Trie {
            head: TrieNode::default(),
        }
    }

    pub(super) fn insert(&mut self, key: &[u8], offset: u64) {
        let mut curr = &mut self.head;
        for c in key.iter() {
            curr = curr.children[*c as usize].get_or_insert_with(|| Box::new(TrieNode::default()));
        }
        curr.offset = Some(offset);
    }

    pub(super) fn search(&self, key: &[u8]) -> usize {
        let curr = &self.head;
        let mut best_offset = curr.offset;
        for c in key.iter() {
            if let Some(node) = &curr.children[*c as usize] {}
        }
        todo!()
    }

    pub(super) fn get(&self, key: &[u8]) -> Option<u64> {
        let mut curr = &self.head;

        for c in key.iter() {
            curr = if let Some(node) = &curr.children[*c as usize] {
                node
            } else {
                return None;
            }
        }

        curr.offset
    }
}

fn rightmost_search(node: &TrieNode) -> Option<u64> {
    todo!()
}
