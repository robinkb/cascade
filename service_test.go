package cascade_test

import (
	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/store/inmemory"
)

func newTestRepository() (cascade.RepositoryService, cascade.MetadataStore, cascade.BlobStore) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)

	return service, metadata, blobs
}
