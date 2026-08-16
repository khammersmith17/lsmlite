use super::SSTable;
use crate::disk::{DiskRecord, decode};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SSTableIterator {
    buffer: Vec<u8>,
    offset: usize,
}

impl SSTableIterator {
    pub async fn new(table: Arc<Mutex<SSTable>>) -> SSTableIterator {
        let buffer = {
            let mut handle = table.lock().await;
            let data_block_end = handle.index.data_block_end;
            let fd: &mut File = &mut handle.fd;
            // TODO: Figure out better approach here to handle IO errors.
            let _ = fd.seek(SeekFrom::Start(6));
            let mut buffer = vec![0_u8; data_block_end as usize - 6_usize];

            // TODO: Figure out better approach here to handle IO errors.
            let _ = fd.read_exact(&mut buffer);
            buffer
        };
        SSTableIterator {
            buffer,
            offset: 0_usize,
        }
    }
}

impl Iterator for SSTableIterator {
    type Item = DiskRecord;
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.buffer.len() {
            return None;
        }

        decode::decode_disk_record(&self.buffer, &mut self.offset)
    }
}
