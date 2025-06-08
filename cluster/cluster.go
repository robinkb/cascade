package cluster

type (
	// Controller manages Nodes.
	Controller struct{}

	// Node represents a clustered node.
	Node interface {
		Start()
		ClusterStatus() Status
		Propose(op Operation) error
		// Consumers must call Handle() to register a function that processes operations.
		Handle(op Operation, f HandlerFunc)
	}

	HandlerFunc func(op Operation) error

	Status struct {
		Clustered bool
	}

	Operation interface {
		ID() uint64
	}
)
