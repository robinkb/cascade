package cluster

type (
	// Controller manages Nodes.
	Controller struct{}

	// Node represents a clustered node.
	Node interface {
		Start()
		ClusterStatus() Status
		Propose(op Operation) error
		// Consumers must call Process() to register a function that processes operations.
		Process(op Operation, f ProcessFunc)
	}

	ProcessFunc func(op Operation) error

	Status struct {
		Clustered bool
	}

	Operation interface {
		ID() uint64
	}
)
