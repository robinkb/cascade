package cluster

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"reflect"
)

var (
	ErrDuplicateProposalType = errors.New("handler for proposal type already registered")
	ErrUnknownProposalType   = errors.New("no registered handler for proposal type")
)

type (
	// Proposal represents an operation that is sent to the cluster.
	// Users are expected to define their own structs that embed ProposalBase
	// to satisfy this interface.
	Proposal interface {
		// ProposalID returns a unique ID for this proposal.
		ProposalID() uint64
	}

	// Response is returned from a HandlerFunc.
	Response interface {
		Error() string
	}

	// HandlerFunc is a function that handles committing a Proposal once it has been
	// accepted by the cluster.
	HandlerFunc func(p Proposal) Response

	// Proposer encapsulates making proposals to a cluster,
	// and handling those proposals once they are accepted.
	Proposer interface {
		// Handle registers a function that will handle accepted proposals.
		// Argument p should be a pointer to a concrete type that implements Proposal.
		// Handle panics if it is called more than once for a single concrete type.
		Handle(p Proposal, f HandlerFunc)
		// Propose makes a proposal to the cluster.
		// Propose panics if a HandlerFunc has not been registered
		// for the given proposal type using Handle.
		Propose(p Proposal) Response
	}
)

// ProposalBase can be embedded into a struct to implement the Proposal interface.
type ProposalBase struct {
	ProposalId uint64
}

func (p *ProposalBase) ProposalID() uint64 {
	if p.ProposalId == 0 {
		p.ProposalId = rand.Uint64()
	}
	return p.ProposalId
}

// ResponseBase can be embedded into a struct to implement the Response interface.
type ResponseBase struct {
	Err error
}

func (r *ResponseBase) Error() string {
	return r.Err.Error()
}

// NewFakeProposer returns a new FakeProposer.
func NewFakeProposer() *FakeProposer {
	return &FakeProposer{
		handlerFuncs: make(map[reflect.Type]HandlerFunc),
	}
}

// FakeProposer handles proposals without actually submitting them to a cluster.
type FakeProposer struct {
	handlerFuncs map[reflect.Type]HandlerFunc

	// Proposals is incremented for every submitted proposal.
	Proposals int64
}

func (p *FakeProposer) Handle(prop Proposal, f HandlerFunc) {
	t := reflect.TypeOf(prop)
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("proposal type already registered: %s", t)
	}
	p.handlerFuncs[t] = f
}

func (p *FakeProposer) Propose(prop Proposal) Response {
	t := reflect.TypeOf(prop)
	f, ok := p.handlerFuncs[t]
	if !ok {
		panic(fmt.Sprintf("unknown proposal type received: %s", t))
	}

	p.Proposals++

	return f(prop)
}
