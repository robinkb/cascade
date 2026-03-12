package fs

import (
	"testing"

	"github.com/robinkb/cascade-registry/registry/store"
	"github.com/robinkb/cascade-registry/registry/store/inmemory"
	storesuite "github.com/robinkb/cascade-registry/registry/store/suite"
	"github.com/stretchr/testify/suite"
)

func TestReconcilerSuite(t *testing.T) {
	suite.Run(t, &storesuite.ReconcilerSuite{
		MetadataStoreConstructor: func() store.Metadata {
			return inmemory.NewMetadataStore()
		},
		BlobStoreConstructor: func() store.Blobs {
			return NewBlobStore(t.TempDir())
		},
	})
}
