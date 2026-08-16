use crate::util;
use std::hash::{DefaultHasher, Hash, Hasher};

fn hash1(key: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    0_u64.hash(&mut hasher);
    hasher.write(key);
    hasher.finish()
}
fn hash2(key: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    1_u64.hash(&mut hasher);
    hasher.write(key);
    hasher.finish()
}

#[derive(Debug)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: u64,
    num_hashes: u64,
}

impl BloomFilter {
    pub fn new(size: usize) -> BloomFilter {
        let num_bits = size * 10;
        let num_words = (num_bits + 63).div_ceil(64);
        let bits = vec![0_u64; num_words];
        let num_bits = 64 * bits.len() as u64;
        let num_hashes =
            (((num_bits as f64 / size as f64) * std::f64::consts::LN_2).round() as u64).max(1);

        BloomFilter {
            bits,
            num_hashes,
            num_bits,
        }
    }

    pub fn from_bytes(buffer: &[u8]) -> BloomFilter {
        let (num_words, bytes_walked) = util::decode_varint(buffer, 0_usize);
        let mut offset = bytes_walked;
        let mut bits: Vec<u64> = Vec::with_capacity(num_words as usize);
        for _ in 0..num_words {
            let be_bytes: [u8; 8] = util::get_be_array8(buffer, offset);
            bits.push(u64::from_be_bytes(be_bytes));
            offset += 8;
        }

        let num_bits = 64_u64 * bits.len() as u64;
        let be_bytes: [u8; 8] = util::get_be_array8(buffer, offset);
        let num_hashes = u64::from_be_bytes(be_bytes);

        BloomFilter {
            bits,
            num_bits,
            num_hashes,
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        let h1 = hash1(key);
        let h2 = hash2(key);

        for i in 0..self.num_hashes {
            let bit_idx = h1.wrapping_add((i).wrapping_mul(h2)) % self.num_bits;
            let word_idx = (bit_idx / 64) as usize;
            let bit_offset = (bit_idx % 64) as usize;
            self.bits[word_idx] |= 1 << bit_offset;
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let h1 = hash1(key);
        let h2 = hash2(key);

        for i in 0..self.num_hashes {
            let bit_idx = h1.wrapping_add((i).wrapping_mul(h2)) % self.num_bits;
            let word_idx = (bit_idx / 64) as usize;
            let bit_offset = (bit_idx % 64) as usize;
            if self.bits[word_idx] & (1 << bit_offset) == 0 {
                return false;
            };
        }
        true
    }

    pub fn serialize(self) -> Vec<u8> {
        let (bit_len_varint, varint_size) = util::encode_varint(self.bits.len());
        let buffer_size = varint_size + (8 * (self.bits.len() + 1));
        let mut buffer = vec![0_u8; buffer_size];
        buffer[..varint_size].copy_from_slice(&bit_len_varint[..varint_size]);
        let mut offset = varint_size;
        for i in 0..self.bits.len() {
            buffer[offset..offset + 8].copy_from_slice(&self.bits[i].to_be_bytes());
            offset += 8;
        }
        buffer[offset..offset + 8].copy_from_slice(&self.num_hashes.to_be_bytes());
        buffer
    }
}
