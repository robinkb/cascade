package registry

import (
	"errors"

	"github.com/robinkb/cascade/registry/repository"
	"github.com/robinkb/cascade/registry/store"
)

type (
	Service interface {
		ListRepositories(count int, last string) ([]string, error)
		CreateRepository(name string) (repository.Service, error)
		GetRepository(name string) (repository.Service, error)
		DeleteRepository(name string) error
	}
)

func New(meta store.Metadata, blobs store.Blobs) Service {
	return &registryService{
		meta:  meta,
		blobs: blobs,
	}
}

type registryService struct {
	meta  store.Metadata
	blobs store.Blobs
}

func (r *registryService) ListRepositories(count int, last string) ([]string, error) {
	return r.meta.ListRepositories(count, last)
}

func (r *registryService) CreateRepository(name string) (repository.Service, error) {
	repo, err := r.meta.CreateRepository(name)
	if err != nil {
		if !errors.Is(err, store.ErrRepositoryExists) {
			return nil, err
		}
	}
	return repository.New(r.blobs, repo), nil
}

func (r *registryService) GetRepository(name string) (repository.Service, error) {
	repo, err := r.meta.GetRepository(name)
	if err != nil {
		if !errors.Is(err, store.ErrRepositoryNotFound) {
			return nil, err
		}
		return r.CreateRepository(name)
	}
	return repository.New(r.blobs, repo), nil
}

func (r *registryService) DeleteRepository(name string) error {
	return r.meta.DeleteRepository(name)
}
