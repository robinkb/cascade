package inmemory

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/robinkb/cascade/registry/store"
	storesuite "github.com/robinkb/cascade/registry/store/suite"
)

func TestReconcilerSuite(t *testing.T) {
	suite.Run(t, &storesuite.ReconcilerSuite{
		MetadataStoreConstructor: func(t *testing.T) store.Metadata {
			return NewMetadataStore()
		},
		BlobStoreConstructor: func(t *testing.T) store.Blobs {
			return NewBlobStore()
		},
	})
}
