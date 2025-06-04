package cluster

import "github.com/robinkb/cascade-registry/store"

type (
	// Controller manages Nodes.
	Controller struct{}

	// Node represents a clustered node.
	Node interface {
		store.Metadata

		Start()
		ClusterStatus() Status
	}

	Status struct {
		Clustered bool
	}
)
