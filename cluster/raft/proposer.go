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
	Operation interface {
		ID() uint64
	}

	HandlerFunc func(op Operation) error

	Proposer interface { // Used by consumers
		Propose(o Operation) error
		// Consumers must call Handle() to register a function that processes operations.
		Handle(op Operation, f HandlerFunc)
		Handler(data []byte)
	}
)

func NewProposer(node raft.Node) *proposer {
	return &proposer{
		raft:         node,
		handlerFuncs: make(map[reflect.Type]HandlerFunc),
		errs: opErrors{
			errs: make(map[uint64]chan error),
		},
	}
}

type proposer struct {
	raft         raft.Node
	handlerFuncs map[reflect.Type]HandlerFunc
	errs         opErrors
}

func (p *proposer) Propose(op Operation) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&op); err != nil {
		log.Panicf("failed to encode operation: %s\n", err)
	}

	errC := p.errs.create(op.ID())
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

func (p *proposer) Handle(op Operation, f HandlerFunc) {
	t := reflect.TypeOf(op)
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("operation already registered: %T", op)
	}
	p.handlerFuncs[reflect.TypeOf(op)] = f
	gob.Register(op)
}

func (p *proposer) Handler(data []byte) {
	buf := bytes.NewBuffer(data)

	var op Operation
	err := gob.NewDecoder(buf).Decode(&op)
	if err != nil {
		log.Panicf("unable to decode as operation: %s", err)
	}

	f, ok := p.handlerFuncs[reflect.TypeOf(op)]
	if !ok {
		log.Panicf("unknown operation received: %T", op)
	}
	err = f(op)

	errC, ok := p.errs.get(op.ID())
	if ok {
		if err != nil {
			errC <- err
		}
		p.errs.delete(op.ID())
	}
}

type opErrors struct {
	mu   sync.RWMutex
	errs map[uint64]chan error
}

func (e *opErrors) create(id uint64) chan error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.errs[id] = make(chan error)
	return e.errs[id]
}

func (e *opErrors) get(id uint64) (chan error, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	err, ok := e.errs[id]
	return err, ok
}

func (e *opErrors) delete(id uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	close(e.errs[id])
	delete(e.errs, id)
}
