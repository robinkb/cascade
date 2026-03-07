/*
QWAL (Queryable Write-Ahead Log) is a storage engine that saves values by type and index.
It was designed as the backend for an on-disk storage implementation for [go.etcd.io/raft/v3].
The design is heavily inspired by Bitcask, but it is not a traditional key/value store.

Applications append values to the DB, which are encoded into records.
The record contains the value's type, its size, a checksum, and the value itself.
Internally, the values are written to files called logs, which are managed by the DB.
There is always one active log, which accepts all new values written to the DB.
Logs are limited in either size or the number of records.
Once a new record would cause the active log to exceed its limits, it is cut.
The cut log is moved into read-only mode.
A new active log is then provisioned, and the value is written to the new log.
A log can be limited either by size, or by record count.

The DB keeps track of all appended values through an internal inventory.
Every time a value is appended, a pointer to that value is added to the inventory.
The pointer tracks which log contains the value, its position within the log, and its size.
The inventory is effectively an in-memory map to each value in the DB.
This allows the DB to hold a large amount of records with a small memory footprint.

Values can only be appended, and not deleted individually.
Instead, the DB is compacted once the maximum log count is reached.
The compaction process deletes the oldest log and its value pointers from the DB.
Compaction is typically triggered after cutting a new log.

Because the DB decides when to cut and compact, applications can register hooks.
These hooks are functions that run when the DB cuts and compacts logs.
Applications can use these hooks to create snapshots, perform internal bookkeeping, etc...
Some applications may also choose to disable the DB's built-in size limits, and to cut and compact manually.

By combining the in-memory inventory and on-disk logs, values can be retrieved from the DB.
Values are retrieved by their type and index within the DB at a specific index, or a range of values.
Applications can query the first (oldest) or last (newest) value of a type.
Compaction shifts the index of each value, with the oldest remaining value of a type moving to index zero.
*/
package qwal
