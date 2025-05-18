package repository_test

import (
	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/store"
	"github.com/robinkb/cascade-registry/store/inmemory"
)

func newTestRepository() (repository.RepositoryService, store.Metadata, store.Blobs) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := repository.NewRepositoryService(metadata, blobs)

	return service, metadata, blobs
}
