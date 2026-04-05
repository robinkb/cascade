package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/robinkb/cascade/cluster"
	"go.etcd.io/raft/v3"
)

func newProposer(node raft.Node) *proposer {
	return &proposer{
		raft:         node,
		handlerFuncs: make(map[cluster.Operation]cluster.HandlerFunc),
		results:      newResultReporter(),
	}
}

// proposer implements cluster.Proposer
type proposer struct {
	raft         raft.Node
	handlerFuncs map[cluster.Operation]cluster.HandlerFunc
	results      resultReporter
}

func (p *proposer) Handle(t cluster.Operation, f cluster.HandlerFunc) {
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("proposal type already registered: %s", t)
	}
	p.handlerFuncs[t] = f
}

func (p *proposer) Propose(req cluster.Request) cluster.Response {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	req.ID = rand.Uint64()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&req); err != nil {
		log.Panicf("failed to encode proposal: %s\n", err)
	}

	resultC := p.results.create(req.ID)
	defer p.results.delete(req.ID)

	err := p.raft.Propose(ctx, buf.Bytes())
	if err != nil {
		return cluster.Response{Err: err}
	}

	select {
	case <-ctx.Done():
		return cluster.Response{Err: ctx.Err()}
	case result := <-resultC:
		return result
	}
}

func (p *proposer) commit(data []byte) {
	buf := bytes.NewBuffer(data)

	var req cluster.Request
	err := gob.NewDecoder(buf).Decode(&req)
	if err != nil {
		log.Panicf("unable to decode as proposal: %s", err)
	}

	f, ok := p.handlerFuncs[req.Op]
	if !ok {
		log.Panicf("unknown proposal type received: %s", req.Op)
	}

	resp := f(req)

	result, ok := p.results.get(req.ID)
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
