package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/robinkb/cascade/cluster"
	"go.etcd.io/raft/v3"
)

func newProposer(node raft.Node) *proposer {
	return &proposer{
		raft:         node,
		handlerFuncs: make(map[reflect.Type]cluster.HandlerFunc),
		results:      newResultReporter(),
	}
}

// proposer implements cluster.Proposer
type proposer struct {
	raft         raft.Node
	handlerFuncs map[reflect.Type]cluster.HandlerFunc
	results      resultReporter
}

func (p *proposer) Handle(prop cluster.Proposal, f cluster.HandlerFunc) {
	t := reflect.TypeOf(prop)
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("proposal type already registered: %s", t)
	}
	p.handlerFuncs[t] = f
	gob.Register(prop)
}

func (p *proposer) Propose(prop cluster.Proposal) cluster.Response {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&prop); err != nil {
		log.Panicf("failed to encode proposal: %s\n", err)
	}

	resultC := p.results.create(prop.ProposalID())
	defer p.results.delete(prop.ProposalID())

	err := p.raft.Propose(ctx, buf.Bytes())
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-resultC:
		return result
	}
}

func (p *proposer) commit(data []byte) {
	buf := bytes.NewBuffer(data)

	var prop cluster.Proposal
	err := gob.NewDecoder(buf).Decode(&prop)
	if err != nil {
		log.Panicf("unable to decode as proposal: %s", err)
	}

	t := reflect.TypeOf(prop)
	f, ok := p.handlerFuncs[t]
	if !ok {
		panic(fmt.Sprintf("unknown proposal type received: %s", t))
	}

	resp := f(prop)

	result, ok := p.results.get(prop.ProposalID())
	if ok {
		result <- resp
	}
}

func newResultReporter() resultReporter {
	return resultReporter{
		results: make(map[uint64]chan cluster.Response),
	}
}

type resultReporter struct {
	mu      sync.Mutex
	results map[uint64]chan cluster.Response
}

func (r *resultReporter) create(id uint64) chan cluster.Response {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results[id] = make(chan cluster.Response)
	return r.results[id]
}

func (r *resultReporter) get(id uint64) (chan cluster.Response, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	resultC, err := r.results[id]
	return resultC, err
}

func (r *resultReporter) delete(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	close(r.results[id])
	delete(r.results, id)
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
