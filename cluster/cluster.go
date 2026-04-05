package cluster

import (
	"io"
	"net/netip"
)

type (
	Operation string

	Request struct {
		ID   uint64
		Op   Operation
		Data []byte
	}

	Response struct {
		Data []byte
		Err  error
	}

	// HandlerFunc is a function that handles committing a proposal type.
	HandlerFunc func(req Request) Response

	// Proposer encapsulates making proposals to a cluster,
	// and handling those proposals once they are accepted.
	Proposer interface {
		// Consumers must call Handle() to register a function that commits
		// proposals of a given operation type.
		Handle(t Operation, f HandlerFunc)
		// Propose makes a proposal to the cluster.
		// Propose panics if a HandlerFunc has not been registered
		// for the given proposal type using Handle.
		Propose(req Request) Response
	}

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
		ID       uint64
		AddrPort netip.AddrPort
	}
)
