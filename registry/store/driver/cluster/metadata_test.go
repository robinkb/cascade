package cluster

import (
	"testing"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/registry/store"
	"github.com/robinkb/cascade/registry/store/driver/inmemory"
	storesuite "github.com/robinkb/cascade/registry/store/suite"
	"github.com/stretchr/testify/suite"
)

func TestMetadataSuite(t *testing.T) {
	suite.Run(t, &storesuite.MetadataSuite{
		Constructor: func(t *testing.T) store.Metadata {
			proposer := cluster.NewFakeProposer()
			meta := inmemory.NewMetadataStore()
			return NewMetadataStore(proposer, meta)
		},
	})
}
