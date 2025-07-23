package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"reflect"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
)

type (
	// Proposal defines an interface for creating proposal types.
	// Implementers must return a stored random ID, but can include additional attributes.
	// These attributes can be retrieved by asserting the Proposal back to its concrete type.
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
)

func newProposer(node raft.Node) *proposer {
	return &proposer{
		raft:         node,
		handlerFuncs: make(map[reflect.Type]HandlerFunc),
		errs: errMap{
			errs: make(map[uint64]chan error),
		},
	}
}

type proposer struct {
	raft         raft.Node
	handlerFuncs map[reflect.Type]HandlerFunc
	errs         errMap
}

func (p *proposer) Handle(proposal Proposal, f HandlerFunc) {
	t := reflect.TypeOf(proposal)
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("proposal type already registered: %T", proposal)
	}
	p.handlerFuncs[reflect.TypeOf(proposal)] = f
	gob.Register(proposal)
}

func (p *proposer) Propose(proposal Proposal) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&proposal); err != nil {
		log.Panicf("failed to encode proposal: %s\n", err)
	}

	errC := p.errs.create(proposal.ID())
	// Propose blocks until accepted by the cluster.
	// TODO: Find out how to retry proposals.
	err := p.raft.Propose(context.TODO(), buf.Bytes())
	if err != nil {
		return err
	}

	select {
	case <-time.Tick(5 * time.Second):
		panic("timed out")
	case <-errC:
		return err
	}
}

// commit decodes the payload into a registered proposal type,
// and calls its HandlerFunc.
//
// commit panics if a HandlerFunc has not been registered
// for the given proposal type using Handle.
func (p *proposer) commit(data []byte) {
	buf := bytes.NewBuffer(data)

	var proposal Proposal
	err := gob.NewDecoder(buf).Decode(&proposal)
	if err != nil {
		log.Panicf("unable to decode as proposal: %s", err)
	}

	f, ok := p.handlerFuncs[reflect.TypeOf(proposal)]
	if !ok {
		log.Panicf("unknown proposal type received: %T", proposal)
	}

	err = f(proposal)

	errC, ok := p.errs.get(proposal.ID())
	if ok {
		if err != nil {
			errC <- err
		}
		p.errs.delete(proposal.ID())
	}
}

type errMap struct {
	mu   sync.RWMutex
	errs map[uint64]chan error
}

func (e *errMap) create(id uint64) chan error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.errs[id] = make(chan error)
	return e.errs[id]
}

func (e *errMap) get(id uint64) (chan error, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	err, ok := e.errs[id]
	return err, ok
}

func (e *errMap) delete(id uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	close(e.errs[id])
	delete(e.errs, id)
}
