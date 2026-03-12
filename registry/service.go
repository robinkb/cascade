package registry

import (
	"errors"

	"github.com/robinkb/cascade-registry/registry/repository"
	"github.com/robinkb/cascade-registry/registry/store"
)

type (
	Service interface {
		CreateRepository(name string) error
		GetRepository(name string) (repository.Service, error)
		DeleteRepository(name string) error
	}
)

func NewService(metadata store.Metadata, blobs store.Blobs) Service {
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

func (r *registryService) GetRepository(name string) (repository.Service, error) {
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

func (r *registryService) DeleteRepository(name string) error {
	return r.metadata.DeleteRepository(name)
}
