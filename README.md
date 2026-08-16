# NLDB
This is an implementation (in all reality a rather naive one) of a LSM Tree style database for fun. Thus, this implement is called Naive LSM Tree Database (NLDB).

## Implementation status
[X] memtable (red black tree)

[X] write ahead logging

[X] sstable serialization with index blocks

[X] bloom filter on disk and in memory

[X] SSTable Index data strucutre on disk and in memory

[X] memtable rotation

[X] handling reads on full in memory memtable until flushed

[X] compaction

[X] reading from multiple SSTables on disk

[X] database statements

[X] database API over TCP

[ ] credential storing

[ ] auth


The database API will be rather simple, support GET, INSERT, and DELETE. This will be a single node to start, then if I want to take it further, probably an eventually consistent distributed set up.

