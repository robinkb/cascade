package cluster

import (
	"fmt"
	"log"
	"math/rand/v2"
)

func NewFakeProposer() Proposer {
	return &fakeProposer{
		handlerFuncs: make(map[Operation]HandlerFunc),
	}
}

// fakeProposer implements the Proposer interface without actually
// making proposals to a  Great for testing.
type fakeProposer struct {
	handlerFuncs map[Operation]HandlerFunc
}

func (p *fakeProposer) Handle(t Operation, f HandlerFunc) {
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("proposal type already registered: %s", t)
	}
	p.handlerFuncs[t] = f
}

func (p *fakeProposer) Propose(req Request) Response {
	req.ID = rand.Uint64()

	f, ok := p.handlerFuncs[req.Op]
	if !ok {
		panic(fmt.Sprintf("unknown proposal type received: %s", req.Op))
	}

	return f(req)
}
