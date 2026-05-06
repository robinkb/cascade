package fake

import (
	"fmt"
	"log"

	"github.com/robinkb/cascade/cluster"
)

// NewProposer returns a new fake Proposer.
func NewProposer() *Proposer {
	return &Proposer{
		handlerFuncs: make(map[cluster.ProposalType]cluster.ProposalFunc),
	}
}

// Proposer handles proposals without actually submitting them to a cluster.
type Proposer struct {
	handlerFuncs map[cluster.ProposalType]cluster.ProposalFunc

	// Proposals is incremented for every submitted proposal.
	Proposals int64
}

func (p *Proposer) Handle(t cluster.ProposalType, f cluster.ProposalFunc) {
	if _, ok := p.handlerFuncs[t]; ok {
		log.Fatalf("%s: %d", cluster.ErrDuplicateProposalType, t)
	}
	p.handlerFuncs[t] = f
}

func (p *Proposer) Propose(t cluster.ProposalType, data []byte) (resp any, err error) {
	f, ok := p.handlerFuncs[t]
	if !ok {
		panic(fmt.Sprintf("%s: %d", cluster.ErrUnknownProposalType, t))
	}

	p.Proposals++
	return f(data)
}

func (p *Proposer) ReadState() error {
	return nil
}
