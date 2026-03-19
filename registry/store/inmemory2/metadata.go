package inmemory2

import (
	"fmt"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
)

func NewMetadataStore() store.Metadata {
	return &metadataStore{
		repositories: make(map[string]*repository),
	}
}

type (
	metadataStore struct {
		store.Metadata

		repositories map[string]*repository
	}

	repository struct {
		blobs map[digest.Digest]any
	}
)

func (m *metadataStore) CreateRepository(name string) (store.Repository, error) {
	if _, ok := m.repositories[name]; ok {
		return nil, fmt.Errorf("%w: %s", store.ErrRepositoryExists, name)
	}

	repo := &repository{
		blobs: make(map[digest.Digest]any),
	}
	m.repositories[name] = repo
	return newRepository(repo), nil
}

func (m *metadataStore) GetRepository(name string) (store.Repository, error) {
	if repo, ok := m.repositories[name]; ok {
		return newRepository(repo), nil
	}
	return nil, fmt.Errorf("%w: %s", store.ErrRepositoryNotFound, name)
}

func (m *metadataStore) DeleteRepository(name string) error {
	if _, ok := m.repositories[name]; ok {
		delete(m.repositories, name)
		return nil
	}
	return fmt.Errorf("%w: %s", store.ErrRepositoryNotFound, name)
}

func newRepository(repo *repository) store.Repository {
	return &repositoryStore{
		// blobs: blobs,
		repo: repo,
	}
}

type repositoryStore struct {
	store.Repository

	repo *repository
}

func (r *repositoryStore) GetBlob(digest digest.Digest) error {
	if _, ok := r.repo.blobs[digest]; ok {
		return nil
	}
	return fmt.Errorf("%w: %s", store.ErrRepositoryBlobNotFound, digest)
}

func (r *repositoryStore) PutBlob(digest digest.Digest) error {
	r.repo.blobs[digest] = nil
	return nil
}

func (r *repositoryStore) DeleteBlob(digest digest.Digest) error {
	delete(r.repo.blobs, digest)
	return nil
}
