package cluster

import (
	"testing"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/registry/store"
	"github.com/robinkb/cascade/registry/store/driver/inmemory"
	storesuite "github.com/robinkb/cascade/registry/store/suite"
	"github.com/stretchr/testify/suite"
)

func TestBlobSuite(t *testing.T) {
	suite.Run(t, &storesuite.BlobSuite{
		Constructor: func(t *testing.T) store.Blobs {
			proposer := cluster.NewFakeProposer()
			blobs := inmemory.NewBlobStore()
			return NewBlobStore(proposer, blobs)
		},
	})
}
