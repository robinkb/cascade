package cascade

import (
	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/store"
)

type (
	RegistryService interface {
		GetRepository(name string) (repository.RepositoryService, error)
	}
)

func NewRegistryService(metadata store.Metadata, blobs store.Blobs) RegistryService {
	return &registryService{
		metadata: metadata,
		blobs:    blobs,
	}
}

type registryService struct {
	metadata store.Metadata
	blobs    store.Blobs
}

func (r *registryService) GetRepository(name string) (repository.RepositoryService, error) {
	return repository.NewRepositoryService(r.metadata, r.blobs), nil
}
