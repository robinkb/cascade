package cascade

import (
	"errors"
	"io"

	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/store"
)

type (
	RegistryService interface {
		CreateRepository(name string) error
		GetRepository(name string) (repository.Service, error)
		DeleteRepository(name string) error
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

func (r *registryService) Reconcile(src store.BlobReader) error {
	digests, err := r.metadata.ListBlobs()
	if err != nil {
		return err
	}

	for _, digest := range digests {
		if _, err := r.blobs.StatBlob(digest); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				rd, err := src.BlobReader(digest)
				if err != nil {
					return err
				}

				wr, err := r.blobs.BlobWriter(digest)
				if err != nil {
					return err
				}

				_, err = io.Copy(wr, rd)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	return nil
}
