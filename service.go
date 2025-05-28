package cascade

import (
	"errors"

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
	if err := r.metadata.GetRepository(name); err != nil {
		if !errors.Is(err, store.ErrRepositoryNotFound) {
			return nil, err
		}
		err := r.metadata.CreateRepository(name)
		if err != nil {
			return nil, err
		}
	}
	return repository.NewRepositoryService(r.metadata, r.blobs), nil
}
