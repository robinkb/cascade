package raft

type (
	Type uint32

	ProposalFunc func(data []byte) (resp any, err error)

	Proposer interface {
		Handle(t Type, f ProposalFunc)
		// Accepts bytes because it must be sent over the wire.
		// Returns any because the response is local.
		Propose(t Type, data []byte) (resp any, err error)
	}
)
