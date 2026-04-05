package inmemory

import (
	"testing"

	"github.com/robinkb/cascade/registry/store"
	storesuite "github.com/robinkb/cascade/registry/store/suite"
	"github.com/stretchr/testify/suite"
)

func TestBlobSuite(t *testing.T) {
	suite.Run(t, &storesuite.BlobSuite{
		Constructor: func(t *testing.T) store.Blobs {
			return NewBlobStore()
		},
	})
}
