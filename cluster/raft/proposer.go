package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/robinkb/cascade-registry/cluster"
	"go.etcd.io/raft/v3"
)

func newProposer(node raft.Node) *proposer {
	return &proposer{
		raft:         node,
		handlerFuncs: make(map[reflect.Type]cluster.HandlerFunc),
		errs:         newErrCs(),
	}
}

// proposer implements cluster.Proposer
type proposer struct {
	raft         raft.Node
	handlerFuncs map[reflect.Type]cluster.HandlerFunc
	errs         errCs
}

func (p *proposer) Handle(proposal cluster.Proposal, f cluster.HandlerFunc) {
	t := reflect.TypeOf(proposal)
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("proposal type already registered: %T", proposal)
	}
	p.handlerFuncs[reflect.TypeOf(proposal)] = f
	gob.Register(proposal)
}

// TODO: Pass a context here.
func (p *proposer) Propose(proposal cluster.Proposal) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&proposal); err != nil {
		log.Panicf("failed to encode proposal: %s\n", err)
	}

	errC := p.errs.create(proposal.ID())
	defer p.errs.delete(proposal.ID())

	// Propose blocks until accepted by the cluster.
	err := p.raft.Propose(context.TODO(), buf.Bytes())
	if err != nil {
		return err
	}

	select {
	// TODO: And wait for ctx.Done() instead of this dumb timeout.
	case <-time.Tick(5 * time.Second):
		panic("timed out")
	case err := <-errC:
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

	var proposal cluster.Proposal
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
		errC <- err
	}
}

func newErrCs() errCs {
	return errCs{
		errs: make(map[uint64]chan error),
	}
}

type errCs struct {
	mu   sync.RWMutex
	errs map[uint64]chan error
}

func (e *errCs) create(id uint64) chan error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.errs[id] = make(chan error)
	return e.errs[id]
}

func (e *errCs) get(id uint64) (chan error, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	err, ok := e.errs[id]
	return err, ok
}

func (e *errCs) delete(id uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	close(e.errs[id])
	delete(e.errs, id)
}
