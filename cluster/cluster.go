package cluster

type (
	// Controller manages Nodes.
	Controller struct{}

	// Node represents a clustered node.
	Node interface {
		Start()
		ClusterStatus() Status
	}

	Status struct {
		Clustered bool
	}
)
