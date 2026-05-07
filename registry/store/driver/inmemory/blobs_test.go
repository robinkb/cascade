package inmemory

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/robinkb/cascade/registry/store"
	storesuite "github.com/robinkb/cascade/registry/store/suite"
)

func TestBlobSuite(t *testing.T) {
	suite.Run(t, &storesuite.BlobSuite{
		Constructor: func(t *testing.T) store.Blobs {
			return NewBlobStore()
		},
	})
}
