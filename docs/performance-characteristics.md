# Performance Characteristics

A summary of some of the challenges faced when optimizing Cascade.

## Raft

The main bottleneck when dealing with Raft is writing entries and hardstate to persistent storage.
Normally small writes are not a problm because of the kernel's pagecache.
Unfortunately, the Raft library recommends that a sync is performed after writing entries to flush the page cache to disk.
This is the main bottleneck when dealing with a large amount of Raft proposals.

The main strategy to increase Raft performance is to decrease the need to sync to disk.
The following sections describe ways to do that.

### Must we always sync?

The Raft library [describes in its README][1]:

>Write Entries, HardState and Snapshot to persistent storage in order, i.e. Entries first, then HardState and Snapshot if they are not empty.

But there is actually an optimization that can be done when the HardState update received from Raft's Ready channel contains no meaningful updates.
This is communicated by the `rd.MustSync` attribute.
If this is not set to `true`, a sync does not have to be performed for the data received.

We also only sync once for both the Entries and HardState, because that's what `etcd`'s WAL does :)
I think it's because when there are Entries received, the HardState contains no meaningful updates.

### Bigger proposals

Entries are created by proposals sent to Raft by the applications.
For blob uploads, the blobs are chunked and encoded into proposals to Raft.
Without any buffering, these end up being chunks of 32 kiB, likely because of the default buffer size of `io.Copy`.
For a 1 GiB blob, that's 32000 chunks.
Each of these chunks will become an entry that has to be synced when it's received.

To decrease the amount of messages, we can implement our own buffering, combining those 32 kiB chunks into bigger proposals.
That decreases the amount of entries to just 1024.
The buffer size is a tradeoff between throughput and memory usage.

### Batching messages

The Raft library actually supports batching multiple entries together to improve throughput.
When multiple proposals are done in parallel (ie. concurrent uploads), their resulting entries can be batched together into one message sent to other nodes.
These batches entries can then all be written to disk together, and synced all at once.

This behavior is controlled by the `MaxSizePerMsg` setting.
Increasing the maximum size can be used to increase throughput at the cost of higher memory usage.

### Or just don't sync?

The argument could be made that syncing at all is silly.
After all, we already have multiple replicas.
If a replica unexpectedly crashes and ends up with a corrupted log, we could get a snapshot from another replica and recover that way.
This would avoid syncing at all, and relying fully on the page cache.

Data loss could occur if all replicas crash at the same time, but that is so unlikely if each replica is in a separate availability zone.
In my situation though, the replicas will not be isolated in that way, and I'd rather not have a corrupt log after a power outage.
But it could be an interesting option to provide.

### Lower level optimizations

There are other lower level optimizations that really get into the weeds of how the page cache and physical storage work.
One thing that etcd's WAL does is padding WAL records to prevent torn writes, for example.
However, I don't consider these necessary with the current level of performance.

[1]: https://github.com/etcd-io/raft?tab=readme-ov-file#usage "Raft README Usage"
