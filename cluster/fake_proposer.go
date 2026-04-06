package cluster

import (
	"fmt"
	"log"
	"math/rand/v2"
)

// NewFakeProposer returns a new FakeProposer.
func NewFakeProposer() *FakeProposer {
	return &FakeProposer{
		handlerFuncs: make(map[Operation]HandlerFunc),
	}
}

// FakeProposer handles proposals without actually submitting them to a cluster.
type FakeProposer struct {
	handlerFuncs map[Operation]HandlerFunc

	// Proposals is incremented for every submitted proposal.
	Proposals int64
}

func (p *FakeProposer) Handle(t Operation, f HandlerFunc) {
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("proposal type already registered: %s", t)
	}
	p.handlerFuncs[t] = f
}

func (p *FakeProposer) Propose(req Request) Response {
	req.ID = rand.Uint64()

	f, ok := p.handlerFuncs[req.Op]
	if !ok {
		panic(fmt.Sprintf("unknown proposal type received: %s", req.Op))
	}

	p.Proposals++

	return f(req)
}
