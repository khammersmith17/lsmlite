use crate::constants;
use crate::disk::encode;
use crate::memtable::inner::{Blob, MemtableInner};
use crate::sstable::bloom_filter::BloomFilter;
use crate::util;
use std::fs::File;
use std::io::Write;

pub fn encode_index_block(index_block: Vec<(Blob, u64)>) -> Vec<u8> {
    let buffer_size: usize = index_block.iter().map(|(key, _)| key.len() + 8 + 2).sum();
    let mut buffer = vec![0_u8; buffer_size];
    let mut offset = 0_usize;

    index_block.iter().for_each(|(key, key_offset)| {
        let (key_len_varint, varint_len) = util::encode_varint(key.len());

        buffer[offset..offset + varint_len].copy_from_slice(&key_len_varint[..varint_len]);
        offset += varint_len;
        buffer[offset..offset + key.len()].copy_from_slice(key);
        offset += key.len();
        buffer[offset..offset + 8].copy_from_slice(&key_offset.to_be_bytes());
        offset += 8;
    });
    buffer[..offset].to_vec()
}

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
    let mut disk_size = 6_u64;
    let mut index_block: Vec<(Blob, u64)> =
        Vec::with_capacity(table.current_size / constants::DISK_BLOCK_SIZE as usize);
    let mut bloom_filter = BloomFilter::new(table.arena.len());
    let _ = fd.write(&constants::NLDB_SSTABLE_HEADER)?;
    let _ = fd.write(&constants::V0_HEADER.to_be_bytes())?;

    inorder_flush(
        table,
        fd,
        table.root_node,
        &mut disk_size,
        &mut index_block,
        &mut bloom_filter,
    )?;

    let index_block_start = disk_size;
    let index_block_len = index_block.len();
    let index_block_buffer = encode_index_block(index_block);
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
    Ok(())
}

fn inorder_flush(
    table: &MemtableInner,
    fd: &mut File,
    node_idx_opt: Option<usize>,
    disk_size: &mut u64,
    index_block: &mut Vec<(Blob, u64)>,
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
        index_block,
        bloom_filter,
    )?;
    {
        // flush current node
        let current_node = &table.arena[node_idx];
        let before = *disk_size % constants::DISK_BLOCK_SIZE;
        let record_offset = *disk_size;
        let disk_record = encode::encode_memtable_node(current_node);
        bloom_filter.insert(&current_node.key);
        *disk_size += disk_record.len() as u64;
        let after = *disk_size % constants::DISK_BLOCK_SIZE;

        if before > after || record_offset == constants::HEADER_SIZE {
            index_block.push((current_node.key.clone(), record_offset));
        }
        let _ = fd.write(&disk_record)?;
    }
    inorder_flush(
        table,
        fd,
        table.arena[node_idx].right,
        disk_size,
        index_block,
        bloom_filter,
    )?;

    Ok(())
}
