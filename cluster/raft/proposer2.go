package raft

type (
	ID   uint64 // implementation detail
	Type uint32

	ProposalFunc func(data []byte) (resp []byte, err error)

	Proposer interface {
		Handle(t Type, f ProposalFunc)
		Propose(t Type, data []byte) (resp []byte, err error)
	}
)
