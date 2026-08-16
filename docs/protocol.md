# nldb Protocol

nldb is an LSM-tree key-value store. This document describes the client request
protocol, the on-disk file formats (WAL and SSTable), and the internal storage
architecture.

---

## Client Request Protocol

All messages (requests and responses) are length-framed: a 4-byte big-endian
`u32` precedes every message body and gives the byte length of that body. The
receiver reads the 4-byte frame header first, then reads exactly that many bytes,
handling TCP partial reads transparently.

```
[msg_len_u32_be][body]
```

The `msg_len` value covers only the body — it does not include the 4 bytes of
the frame header itself.

### Request body format

Request bodies are binary messages with a text command token followed by
varint-length-prefixed fields. All varints use the same unsigned LEB128 encoding
described below.

```
<command> <SP> <key_len_varint><key> [<value_len_varint><value>]
```

- `<command>` — ASCII text, case-insensitive (`GET`, `INSERT`, `DELETE`)
- `<SP>` — a single space byte (`0x20`) separating the command from the payload
- `<key_len_varint>` — varint-encoded byte length of the key
- `<key>` — raw UTF-8 key bytes (spaces allowed, length is not space-terminated)
- `<value_len_varint>` — varint-encoded byte length of the value (INSERT only)
- `<value>` — raw value bytes, may be arbitrary binary

### Commands

#### GET

Retrieve the value stored at `key`. Returns nothing if the key does not exist or
has been deleted.

```
GET <key_len_varint><key>
```

Example — get key `"foo"` (key length 3, fits in one varint byte `0x03`),
with a 4-byte frame header (`0x00 0x00 0x00 0x08` = 8 body bytes):

```
00 00 00 08  47 45 54 20  03  66 6F 6F
[frame len]  G  E  T  SP  3   f  o  o
```

#### INSERT

Store `value` at `key`, overwriting any existing value.

```
INSERT <key_len_varint><key><value_len_varint><value>
```

Example — insert key `"foo"`, value `"bar"`, frame header
(`0x00 0x00 0x00 0x0F` = 15 body bytes):

```
00 00 00 0F  49 4E 53 45 52 54 20  03  66 6F 6F  03  62 61 72
[frame len]  I  N  S  E  R  T  SP  3   f  o  o   3   b  a  r
```

#### DELETE

Write a tombstone for `key`. Subsequent reads for this key return nothing.

```
DELETE <key_len_varint><key>
```

### Error conditions

The parser returns `NldbError::InvalidQuery` for any of the following:

- Unknown command token
- No space after the command (payload too short)
- Key length varint is 0 (empty keys are rejected)
- Key length varint extends beyond the buffer
- Key length field declares more bytes than remain in the buffer
- Value length field declares more bytes than remain in the buffer (INSERT)
- Key bytes are not valid UTF-8
- A varint with more than 10 continuation bytes (malformed / overflow protection)

---

## Server Response Protocol

### Success response

#### GET

```
[0x01][blob_len_varint][blob]
```

The `blob_len_varint` encodes the byte length of the value. If the key does not
exist or has been deleted, `blob_len_varint` is `0` and no blob bytes follow.

#### INSERT and DELETE

```
[0x01]
```

A single success byte with no additional payload.

### Error response

```
[0x00][error_code_varint]
```

| Code | Meaning |
|------|---------|
| `0x01` | Internal error |
| `0x02` | Invalid request (parse error) |
| `0x03` | Key not found |
| `0x04` | Key exceeds size constraint |
| `0x05` | Record exceeds size constraint |

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
wal.<timestamp_nanos>.log
```

The timestamp is nanoseconds since the Unix epoch. Multiple WAL files can exist
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

Each record is one of two types, identified by a header byte.

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
<timestamp_nanos>.sstable
```

### File layout

```
+------------------+
|  File Header (6) |
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

### File header (6 bytes)

```
[78, 76, 68, 66][version_u16_be]
  "NLDB"       currently 0x0000
```

| Field   | Size | Description               |
|---------|------|---------------------------|
| magic   | 4    | `0x4E 0x4C 0x44 0x42` ("NLDB") |
| version | 2    | big-endian u16, currently `0` |

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

### Write path

1. Client sends a request; the parser produces an `NldbRequest`.
2. The write is applied to the **active memtable** (red-black tree) and
   appended to the **WAL**.
3. If the memtable is full (`TableFull`):
   - The active memtable is rotated into the **immutable memtable queue**.
   - A `MemtableFlushSignal::Flush` is sent to the background flush task.
   - A fresh memtable is created and the write is retried.
4. The write-through **LRU cache** is evicted for the key on any write or
   delete.

### Read path (newest-first)

1. Check the **LRU cache** — return immediately on hit.
2. Check the **active memtable**.
3. Check the **immutable memtable queue** (newest to oldest).
4. Check the **SSTable cache** (newest SSTable first).
5. A tombstone at any layer stops the search and returns nothing.

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

- `.sstable` files — loaded into the SSTable cache, sorted newest-first by
  timestamp in the filename.
- `wal.<ts>.log` files — sorted oldest-first by timestamp. The caller handles
  each category differently:
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
