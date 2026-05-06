package cluster

import (
	"errors"
)

var (
	ErrDuplicateProposalType = errors.New("handler for proposal type already registered")
	ErrUnknownProposalType   = errors.New("no registered handler for proposal type")
)

type (
	ProposalType uint32
	// ProposalFunc handles committing a proposal once it has been accepted by the cluster.
	ProposalFunc func(data []byte) (resp any, err error)

	// Proposer encapsulates making proposals to a cluster,
	// and handling those proposals once they are accepted.
	Proposer interface {
		// Handle registers a function that will handle accepted proposals for the given type.
		// Handle panics if it is called more than once for a single type.
		Handle(t ProposalType, f ProposalFunc)
		// Propose makes a proposal to the cluster.
		// Propose panics if a HandlerFunc has not been registered
		// for the given proposal type using Handle.
		Propose(t ProposalType, data []byte) (resp any, err error)
		// ReadState waits until the Proposer is caught up with the rest of the cluster
		// before proceeding.
		ReadState() error
	}
)
