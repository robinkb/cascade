package cluster

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/robinkb/cascade/cluster/fake"
	"github.com/robinkb/cascade/registry/store"
	"github.com/robinkb/cascade/registry/store/driver/inmemory"
	storesuite "github.com/robinkb/cascade/registry/store/suite"
)

func TestMetadataSuite(t *testing.T) {
	suite.Run(t, &storesuite.MetadataSuite{
		Constructor: func(t *testing.T) store.Metadata {
			proposer := fake.NewProposer()
			meta := inmemory.NewMetadataStore()
			return NewMetadataStore(proposer, meta)
		},
	})
}
