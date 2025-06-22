package raft

/**
File for sketching interfaces.
*/

type (
	Operation interface {
		ID() uint64
	}

	HandlerFunc func(op Operation) error

	Proposer interface { // Used by consumers
		Propose(o Operation) error
		// Consumers must call Handle() to register a function that processes operations.
		Handle(op Operation, f HandlerFunc)
	}

	Node interface { // Represents everything that a Raft node has to do for Raft to work
		// Lifecycle
		Start()
		Stop()
		Tick()
		ClusterStatus() Status
	}

	Status struct {
		Clustered bool
	}
)
