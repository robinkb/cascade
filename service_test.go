package cascade_test

import (
	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/store/inmemory"
)

func newTestRegistry() (cascade.RegistryService, cascade.MetadataStore, cascade.BlobStore) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)

	return service, metadata, blobs
}
