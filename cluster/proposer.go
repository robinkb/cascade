package cluster

import (
	"errors"
	"fmt"
	"log"
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
		//  for the given proposal type using Handle.
		Propose(t ProposalType, data []byte) (resp any, err error)
	}
)

// NewFakeProposer returns a new FakeProposer.
func NewFakeProposer() *FakeProposer {
	return &FakeProposer{
		handlerFuncs: make(map[ProposalType]ProposalFunc),
	}
}

// FakeProposer handles proposals without actually submitting them to a cluster.
type FakeProposer struct {
	handlerFuncs map[ProposalType]ProposalFunc

	// Proposals is incremented for every submitted proposal.
	Proposals int64
}

func (p *FakeProposer) Handle(t ProposalType, f ProposalFunc) {
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("proposal type already registered: %s", t)
	}
	p.handlerFuncs[t] = f
}

func (p *FakeProposer) Propose(t ProposalType, data []byte) (resp any, err error) {
	f, ok := p.handlerFuncs[t]
	if !ok {
		panic(fmt.Sprintf("unknown proposal type received: %s", t))
	}

	p.Proposals++

	return f(data)
}
