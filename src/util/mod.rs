use crate::error::NldbError;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const LEAST_BYTE_USIZE: usize = 0x7F;
const LEAST_BYTE_U8: u8 = 0x7F;
const CONTINUATION: u8 = 0x80;

pub fn get_be_array8(buffer: &[u8], offset: usize) -> [u8; 8] {
    buffer[offset..offset + 8]
        .try_into()
        .expect("Invalid size slice when deserializing bloom filter")
}

pub fn get_be_array2(buffer: Vec<u8>) -> [u8; 2] {
    buffer
        .try_into()
        .expect("Invalid size slice when deserializing bloom filter")
}

pub fn generate_wal_file_name() -> PathBuf {
    let start_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("wal.{start_ts}.log").into()
}

pub fn generate_sstable_file_name() -> PathBuf {
    let start_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{start_ts}.sstable").into()
}

// Given a length of a block on disk, derive the number of bytes it takes to represent the varint
// for the length of this block.
pub fn varint_size_from_len(len: usize) -> usize {
    let mut size = 1_usize;

    let mut upper_bound = 1_usize << 7;

    while len >= upper_bound && size <= 10 {
        size += 1;
        upper_bound <<= 7;
    }

    debug_assert!(size <= 10_usize);
    size
}

pub fn encode_varint(mut value: usize) -> ([u8; 10], usize) {
    let mut buffer = [0_u8; 10];
    let mut idx = 0_usize;

    loop {
        let mut byte = (value & LEAST_BYTE_USIZE) as u8;
        // shift passed this varint byte
        value >>= 7;

        if value != 0 {
            // set continuation bit
            byte |= CONTINUATION;
            buffer[idx] = byte;
            idx += 1;
        } else {
            buffer[idx] = byte;
            idx += 1;
            break;
        }
    }
    (buffer, idx)
}

pub fn decode_untrusted_varint(
    buffer: &[u8],
    mut offset: usize,
) -> Result<(u64, usize), NldbError> {
    let end = buffer.len();
    let mut varint = 0_u64;
    let start = offset;

    loop {
        if offset >= end || offset - start >= 10 {
            return Err(NldbError::InvalidQuery);
        }
        let byte = buffer[offset];
        varint |= ((byte & LEAST_BYTE_U8) as u64) << (7 * (offset - start));
        offset += 1;

        if byte & CONTINUATION == 0 {
            break;
        }
    }
    Ok((varint, offset - start))
}

pub fn decode_varint(buffer: &[u8], mut offset: usize) -> (u64, usize) {
    let mut varint = 0_u64;
    let start = offset;

    loop {
        let byte = buffer[offset];
        varint |= ((byte & LEAST_BYTE_U8) as u64) << (7 * (offset - start));
        offset += 1;

        if byte & CONTINUATION == 0 {
            break;
        }
    }
    (varint, offset - start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_single_byte_value() {
        let (buf, len) = encode_varint(1);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 1);
    }

    #[test]
    fn encode_zero() {
        let (buf, len) = encode_varint(0);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 0);
    }

    #[test]
    fn encode_max_single_byte() {
        // 127 fits in one varint byte
        let (buf, len) = encode_varint(127);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 0x7F);
    }

    #[test]
    fn encode_two_byte_boundary() {
        // 128 requires two varint bytes
        let (buf, len) = encode_varint(128);
        assert_eq!(len, 2);
        assert_eq!(buf[0], 0x80); // 0 | continuation
        assert_eq!(buf[1], 0x01);
    }

    #[test]
    fn encode_large_value() {
        let (buf, len) = encode_varint(300);
        assert_eq!(len, 2);
        // 300 = 0b1_0010110_0 → bytes: 0xAC 0x02
        assert_eq!(buf[0], 0xAC);
        assert_eq!(buf[1], 0x02);
    }

    #[test]
    fn decode_single_byte() {
        let buf = [0x01_u8];
        let (val, walked) = decode_varint(&buf, 0);
        assert_eq!(val, 1);
        assert_eq!(walked, 1);
    }

    #[test]
    fn decode_zero() {
        let buf = [0x00_u8];
        let (val, walked) = decode_varint(&buf, 0);
        assert_eq!(val, 0);
        assert_eq!(walked, 1);
    }

    #[test]
    fn decode_two_byte() {
        let buf = [0x80_u8, 0x01];
        let (val, walked) = decode_varint(&buf, 0);
        assert_eq!(val, 128);
        assert_eq!(walked, 2);
    }

    #[test]
    fn decode_at_offset() {
        let buf = [0xFF_u8, 0x01, 0x00];
        // skip first byte, decode from offset 1
        let (val, walked) = decode_varint(&buf, 1);
        assert_eq!(val, 1);
        assert_eq!(walked, 1);
    }

    #[test]
    fn encode_decode_roundtrip() {
        for value in [0, 1, 127, 128, 300, 16383, 16384, usize::MAX >> 1] {
            let (buf, len) = encode_varint(value);
            let (decoded, walked) = decode_varint(&buf, 0);
            assert_eq!(decoded as usize, value, "roundtrip failed for {value}");
            assert_eq!(walked, len);
        }
    }

    #[test]
    fn get_be_array8_reads_correct_slice() {
        let buf: Vec<u8> = (0..16).collect();
        let arr = get_be_array8(&buf, 4);
        assert_eq!(arr, [4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn get_be_array2_converts_correctly() {
        let buf = vec![0x01_u8, 0x00];
        let arr = get_be_array2(buf);
        assert_eq!(u16::from_be_bytes(arr), 256);
    }

    #[test]
    fn test_varint_size_from_len() {
        let r1 = varint_size_from_len(120);
        assert_eq!(r1, 1);

        let r2 = varint_size_from_len(128);
        assert_eq!(r2, 2);

        let r3 = varint_size_from_len(250);
        assert_eq!(r3, 2);

        let r4 = varint_size_from_len(16383);
        assert_eq!(r4, 2);

        let r4 = varint_size_from_len(16384);
        assert_eq!(r4, 3);
    }
}
