# lsmlite Protocol

lsmlite is an LSM-tree key-value store. This document describes the client request
protocol, the on-disk file formats (WAL and SSTable), and the internal storage
architecture.

---


## Varint Encoding (LEB128)

All length fields use unsigned LEB128 (little-endian base-128) varints, the same
format as Protocol Buffers and the WAL/SSTable on-disk format.

Each byte encodes 7 bits of the value. The high bit (`0x80`) is a continuation
flag — set means another byte follows, clear means this is the final byte.

```
Value 1   → 0x01
Value 127 → 0x7F
Value 128 → 0x80 0x01
Value 300 → 0xAC 0x02
```

A valid varint is at most 10 bytes (sufficient for any u64).

---

## Write-Ahead Log (WAL)

Every write to the active memtable is first appended to a WAL file before
insertion into the in-memory tree. On a crash and restart the WAL is replayed to
recover the memtable state.

### File naming

```
lsmlite.wal.<timestamp>.log
```

The timestamp is milliseconds since the Unix epoch. Multiple WAL files can exist
simultaneously — one per memtable that has been rotated into the immutable queue
but not yet flushed to an SSTable. When a memtable is successfully flushed to an
SSTable its WAL file is deleted. WAL deletion failure is treated as fatal and
poisons the database.

### WAL buffering

Writes are buffered in memory and flushed to disk when either:

- The buffer reaches **64 KB**, or
- **400 ms** has elapsed since the last flush.

The buffer capacity is pre-allocated at 100 KB.

### Record format

Each record is one of two types, identified by a header byte. Records are encoded
once at creation time and stored in memory in the same binary format used on disk
(single-copy design — see [Storage Architecture](#storage-architecture)).

#### Data record (`header = 0x00`)

```
[0x00][log_size_varint][key_len_varint][key][data_len_varint][data]
```

| Field            | Type          | Description                        |
|------------------|---------------|------------------------------------|
| header           | u8            | `0x00` = data record               |
| log_size         | varint        | total byte length of remaining fields |
| key_len          | varint        | byte length of key                 |
| key              | UTF-8 bytes   | key string                         |
| data_len         | varint        | byte length of value               |
| data             | bytes         | raw value bytes                    |

#### Tombstone record (`header = 0x01`)

```
[0x01][log_size_varint][key_len_varint][key]
```

| Field    | Type        | Description              |
|----------|-------------|--------------------------|
| header   | u8          | `0x01` = tombstone       |
| log_size | varint      | total byte length of remaining fields |
| key_len  | varint      | byte length of key       |
| key      | UTF-8 bytes | key string               |

---

## SSTable File Format

When a memtable is full it is flushed to an immutable SSTable file on disk.
Records are written in key-sorted order (in-order traversal of the red-black
tree).

### File naming

```
lsmlite.<timestamp>.sstable
```

### File layout

```
+------------------+
|  File Header (9) |
+------------------+
|  Data Blocks     |  variable length, sorted key-value records
+------------------+
|  Index Block     |  sparse index: one entry per 4 KB data block boundary
+------------------+
|  Bloom Filter    |  membership filter over all keys
+------------------+
|  Footer (24)     |  byte offsets for index block, index count, bloom filter
+------------------+
```

### File header (9 bytes)

```
[6C 73 6D 6C 69 74 65][version_u16_be]
      "lsmlite"          currently 0x0000
```

| Field   | Size | Description                          |
|---------|------|--------------------------------------|
| magic   | 7    | ASCII `"lsmlite"` (`0x6C736D6C697465`) |
| version | 2    | big-endian u16, currently `0`        |

### Data block records

Data blocks use the same binary encoding as WAL records (data record and
tombstone record formats above). Records are packed contiguously in sorted key
order. No padding is added between records.

Block boundaries occur every **4 KB** of data. The index block records the key
and file offset at each boundary.

### Index block

The index block is a sparse index with one entry per 4 KB data block. Each
entry maps the first key in a block to its byte offset within the file. Entries
are written in sorted key order.

```
[key_len_varint][key][offset_u64_be] ...
```

| Field      | Type        | Description                       |
|------------|-------------|-----------------------------------|
| key_len    | varint      | byte length of key                |
| key        | UTF-8 bytes | first key of the block            |
| offset     | u64 big-endian | byte offset of the block start |

### Bloom filter

A bloom filter over all keys in the SSTable, serialized immediately after the
index block. Used to skip SSTables that definitely do not contain a key before
performing any disk I/O on the data blocks.

### Footer (24 bytes)

Three big-endian u64 values at a fixed offset from the end of the file:

```
[index_block_start_u64_be][index_block_count_u64_be][bloom_filter_start_u64_be]
```

| Field               | Size | Description                              |
|---------------------|------|------------------------------------------|
| index_block_start   | 8    | byte offset of first index block entry   |
| index_block_count   | 8    | number of entries in the index block     |
| bloom_filter_start  | 8    | byte offset of the bloom filter          |

---

## Storage Architecture

### Single-copy record design

Records are encoded into their on-disk binary format once at creation time and
stored in memory in that same format. The in-memory `Record` struct holds the
encoded `blob` plus two offset fields (`key: (usize, usize)` and
`value: Option<(usize, usize)>`) that index into the blob, providing zero-copy
access to key and value bytes without a separate decode step. Flushing a memtable
to the WAL or an SSTable is a direct write of these blobs — no re-encoding occurs.

Tombstones and data records are distinguished solely by the `value` field:
`Some((offset, len))` = data, `None` = tombstone.

### Write path

1. `LsmLite::set(key, value)`.
2. The write is applied to the **active memtable** (red-black tree) and
   appended to the **WAL**.
3. If the memtable is full (`TableFull`):
   - The active memtable is rotated into the **immutable memtable queue**.
   - A `MemtableFlushSignal::Flush` is sent to the background flush task.
   - A fresh memtable is created and the write is retried.

### Delete path

1. `LsmLite::delete(key)`.
2. A tombstone write is applied to the **active memtable** (red-black tree) and
   appended to the **WAL**.
3. If the memtable is full (`TableFull`):
   - The active memtable is rotated into the **immutable memtable queue**.
   - A `MemtableFlushSignal::Flush` is sent to the background flush task.
   - A fresh memtable is created and the write is retried.


### Read path (newest-first)

1. Check the **active memtable**.
2. Check the **immutable memtable queue** (newest to oldest).
3. Check the **SSTable cache** (newest SSTable first).
4. A tombstone at any layer stops the search and returns nothing.

### Background flush task

A single long-running task processes `MemtableFlushSignal` messages serially,
ensuring memtables are flushed to SSTables in order:

1. Receive `Flush` signal.
2. Create a new `.sstable` file and write the oldest immutable memtable to it.
3. Send `CompactionSignal::LoadSSTable` to the compaction background task.
4. Wait for `SSTableLoadAck::Done` before processing the next flush signal.
   This prevents flushing a second table before the first SSTable is loaded
   into the read path.
5. Delete the WAL file associated with the flushed memtable.

### Compaction

When the SSTable cache reaches `compaction_rate` tables, an N-way merge
compaction is triggered:

- All SSTable iterators are merged in key order.
- When duplicate keys exist across tables, the record from the newer table wins.
- Tombstones are dropped from the compacted output (they are no longer needed
  once all older records for that key have been merged away).
- The resulting single SSTable replaces all previous tables in the cache.

### Restart / recovery

On startup, `get_restart_state` scans the working directory for:

- `lsmlite.<ts>.sstable` files — loaded into the SSTable cache, sorted
  newest-first by timestamp in the filename.
- `lsmlite.wal.<ts>.log` files — sorted oldest-first by timestamp. The caller
  handles each category differently:
  - **All WAL files except the newest** represent full memtables that were
    rotated but never flushed. Each is replayed and immediately flushed to a
    new SSTable.
  - **The newest WAL file** represents the active memtable at shutdown, which
    may be partially filled. It is replayed into the new active memtable without
    flushing.

### Poisoned state

If a background flush or WAL deletion fails, an atomic `poisoned` flag is set.
Any subsequent read or write operation panics immediately. This is a
fail-fast strategy to prevent silent data corruption.
