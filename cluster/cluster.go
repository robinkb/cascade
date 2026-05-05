package cluster

import (
	"io"
)

type (
	// Snapshotter wraps the basic Snapshot method.
	Snapshotter interface {
		// Snapshot writes a snapshot of the application's state machine to the given Writer.
		Snapshot(w io.Writer) error
	}

	// Restorer wraps the basic Restore method.
	Restorer interface {
		// Restorer reads a snapshot from the given Reader and applies it to the application's state machine.
		Restore(r io.Reader, peer Peer) error
	}

	Peer struct {
		ID   uint64
		Addr string
	}
)
