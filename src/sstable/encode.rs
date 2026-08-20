use crate::constants;
use crate::memtable::inner::MemtableInner;
use crate::ssindex::trie::Trie;
use crate::sstable::bloom_filter::BloomFilter;
use std::fs::File;
use std::io::Write;

pub fn encode_footer(
    index_block_start: u64,
    index_block_count: u64,
    bloom_filter_start: u64,
) -> Vec<u8> {
    let mut buffer = vec![0_u8; constants::FOOTER_SIZE as usize];
    buffer[..8].copy_from_slice(&index_block_start.to_be_bytes());
    buffer[8..16].copy_from_slice(&index_block_count.to_be_bytes());
    buffer[16..24].copy_from_slice(&bloom_filter_start.to_be_bytes());
    buffer
}

pub fn write_sstable(table: &MemtableInner, fd: &mut File) -> std::io::Result<()> {
    /*
     * Allocate an entire buffer of total size + number of records * 4 for variants.
     * DFS inorder to insert records in sorted order
     * */
    let mut disk_size = constants::HEADER_SIZE;
    let mut trie = Trie::new(table.arena.len());
    let mut bloom_filter = BloomFilter::new(table.arena.len());
    let _ = fd.write(&constants::LSMLITE_SSTABLE_HEADER)?;
    let _ = fd.write(&constants::V0_HEADER.to_be_bytes())?;

    inorder_flush(
        table,
        fd,
        table.root_node,
        &mut disk_size,
        &mut trie,
        &mut bloom_filter,
    )?;

    let index_block_start = disk_size;
    let index_block_buffer = trie.serialize();
    let index_block_len = index_block_buffer.len();
    let bloom_filter_start = index_block_start + index_block_buffer.len() as u64;
    let _ = fd.write(&index_block_buffer)?;
    let bloom_filter_buffer = bloom_filter.serialize();
    let _ = fd.write(&bloom_filter_buffer)?;
    let footer = encode_footer(
        index_block_start,
        index_block_len as u64,
        bloom_filter_start,
    );
    let _ = fd.write(&footer)?;
    fd.flush()?;
    fd.sync_all()?;
    Ok(())
}

fn inorder_flush(
    table: &MemtableInner,
    fd: &mut File,
    node_idx_opt: Option<usize>,
    disk_size: &mut u64,
    trie: &mut Trie,
    bloom_filter: &mut BloomFilter,
) -> std::io::Result<()> {
    let Some(node_idx) = node_idx_opt else {
        return Ok(());
    };

    inorder_flush(
        table,
        fd,
        table.arena[node_idx].left,
        disk_size,
        trie,
        bloom_filter,
    )?;
    {
        // flush current node
        let current_node = &table.arena[node_idx];
        let record_offset = *disk_size;
        let disk_record = current_node.copy_record();
        bloom_filter.insert(&current_node.key());
        *disk_size += disk_record.len() as u64;

        trie.insert(current_node.key(), record_offset);
        let _ = fd.write(&disk_record)?;
    }
    inorder_flush(
        table,
        fd,
        table.arena[node_idx].right,
        disk_size,
        trie,
        bloom_filter,
    )?;

    Ok(())
}
