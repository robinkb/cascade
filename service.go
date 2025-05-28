package cascade

import (
	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/store"
)

type (
	RegistryService interface {
		CreateRepository(name string) error
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

func (r *registryService) CreateRepository(name string) error {
	return r.metadata.CreateRepository(name)
}

func (r *registryService) GetRepository(name string) (repository.RepositoryService, error) {
	return repository.NewRepositoryService(r.metadata, r.blobs), nil
}
