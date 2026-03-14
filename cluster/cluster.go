package cluster

import (
	"io"
	"net/netip"
)

type (
	// Proposal defines an interface for creating proposal types.
	// Implementers must return a stored random ID, but can include additional attributes.
	// These attributes can be retrieved by asserting the Proposal back to its concrete type.
	// TODO: Returning the payload as bytes in a Context or Body method makes way more sense.
	// Body would be more like an HTTP request, while Context would be more like the rest of the Raft library.
	Proposal interface {
		ID() uint64
	}

	// HandlerFunc is a function that handles committing a proposal type.
	// They typically type assert the given proposal into its concrete type,
	// and process the payload by committing it to the state machine.
	// HandlerFuncs can assume that they are never passed a Proposal of the wrong concrete type.
	HandlerFunc func(p Proposal) error

	// Proposer encapsulates making proposals to the cluster,
	// and handling those proposals once they are accepted.
	Proposer interface {
		// Consumers must call Handle() to register a function that commits
		// proposals of a certain concrete type.
		Handle(p Proposal, f HandlerFunc)
		// Propose makes a proposal to the Raft log.
		//
		// Propose panics if a HandlerFunc has not been registered
		// for the given proposal type using Handle.
		Propose(p Proposal) error
	}

	// SnapshotRestorer combines the basic Snapshotter and Restorer interfaces.
	SnapshotRestorer interface {
		Snapshotter
		Restorer
	}

	// Snapshotter wraps the basic Snapshot method.
	Snapshotter interface {
		// Snapshot writes a snapshot of the application's state machine to the given Writer.
		Snapshot(w io.Writer) error
	}

	// Restorer wraps the basic Restore method.
	Restorer interface {
		// Restorer reads a snapshot from the given Reader and applies it to the application's state machine.
		Restore(r io.Reader, peers []Peer) error
	}

	Peer struct {
		ID       uint64
		AddrPort netip.AddrPort
	}
)
