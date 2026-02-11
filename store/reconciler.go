package store

import (
	"errors"
	"io"
)

/**
Process should be:

1. Restore Metadata from snapshot
2. Reconcile Blobs based on Metadata
3. Mark as ready to participate in cluster

All of these steps are linked, but that doesn't have to be reflected in the interface.

Reconciliation is an implementation detail of the blobstore, though. Raft should not need to know about it.
It could also be useful in use-cases outside of Raft? Maybe?
In any case, it would make testing easier.
But reconciliation also has to know about other nodes... Which is something that's part of Raft.
It's inherently a process that _requires_ clustering.
So that feels like it should be part of the cluster package.

Snapshotter interface as an optional extension of MetadataStore makes sense.
Backup and restore, pretty simple.

Syncer interface should provide extra methods for whatever I need
to sync blob stores between nodes. This should be separate from the regular BlobStore interface,
because none of these methods should go through Raft.

How does the restore process actually work in Raft though? Is peer info gotten from Raft?
Or does it rely on the commandline flags, and would thus depend on the discovery service?
--> At least in raftexample, snapshots are sent over Raft, so when it starts, we're already in the cluster.

Got it!

- cluster package provides a Snapshotter interface with Snapshot and Restore
- store packages provides the Snapshotter and Syncer interfaces shown below.
- Reconciler in the store package implements cluster.Snapshotter based on the interfaces below.
*/

func Reconcile(meta Metadata, src, dst Blobs) error {
	digests, err := meta.ListBlobs()
	if err != nil {
		return err
	}

	for _, d := range digests {
		if _, err := dst.StatBlob(d); err != nil {
			if errors.Is(err, ErrNotFound) {
				r, err := src.BlobReader(d)
				if err != nil {
					return err
				}

				w, err := dst.BlobWriter(d)
				if err != nil {
					return err
				}

				_, err = io.Copy(w, r)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	return nil
}
