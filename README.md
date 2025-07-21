# Cascade Registry

## Developing

There are a few generated mocks in the `testing/mock` package.
These mocks are generated with [Mockery](https://vektra.github.io/mockery/latest/).

To update the mocks, install Mockery using the [installation instructions](https://vektra.github.io/mockery/latest/installation/).
After installation simply run `mockery` to regenerate the mocks.
The Mockery configuration in [`.mockery.yaml`](/.mockery.yaml) has lots of comments.

Trying to push library/fedora:42 (one layer, about 60 MiB)

Before optimizations: 37s
Combine writing Entries and HardState: 37s?!
Use raft.MustSync: 22s  -> Still too slow, but big improvement
Use rd.MustSync instead: 22s -> Same, but I like using what Raft includes
Turn off sync on Bolt: 22s -> As expected, streaming uploads are not heavy on metadata store

I think at this point I have to decrease the amount of entries by buffering
My tests do hit the SSD extra hard, because all instances run on the same machine
Could also look into the really low-level optimizations, like padding to prevent torn writes...

Or... Don't sync on every entry. Sync a log when it is being cut, and sync snapshots.


Increase MaxSizePerMsg to 1 MiB: 22s -> Still seeing only 1 entry per batch in the log; maybe batching only works for parallel writes? Saw it with elasticsearch but not fedora (which is only 1 layer)
Implement 1MiB buffered proposer: 2s -> boom
Buffered proposer with 256kiB buffer: 4s -> Clear correlation in performance. Accidentally made an implementation that buffered the whole image, and that was almost instant.

Re-runs with time command:
256 kiB buffer: 4.13s   --> 14.5 MiB/s
512 kiB buffer: 3.08s   -->
1 MiB buffer:   2.35s   -->
2 MiB buffer:   1.85s   -->
4 MiB buffer:   1.68s   -->
