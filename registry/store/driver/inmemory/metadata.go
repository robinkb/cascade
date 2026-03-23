package inmemory2

import (
	"fmt"
	"iter"
	"sync"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
)

func NewMetadataStore() store.Metadata {
	return &metadataStore{
		blobs: &blobs{
			digests: make(map[digest.Digest]owners),
		},
		repositories: make(map[string]*repository),
	}
}

type (
	metadataStore struct {
		store.Metadata

		blobs        *blobs
		repositories map[string]*repository
	}

	blobs struct {
		mu      sync.RWMutex
		digests map[digest.Digest]owners
	}

	owners struct {
		repositories map[string]any
	}

	repository struct {
		blobs     map[digest.Digest]any
		manifests map[digest.Digest]manifest
	}

	manifest struct {
		metadata   store.Manifest
		references store.References
	}
)

func (m *metadataStore) CreateRepository(name string) (store.Repository, error) {
	if _, ok := m.repositories[name]; ok {
		return nil, fmt.Errorf("%w: %s", store.ErrRepositoryExists, name)
	}

	repo := &repository{
		blobs:     make(map[digest.Digest]any),
		manifests: make(map[digest.Digest]manifest),
	}
	m.repositories[name] = repo
	return newRepository(name, m.blobs, repo), nil
}

func (m *metadataStore) GetRepository(name string) (store.Repository, error) {
	if repo, ok := m.repositories[name]; ok {
		return newRepository(name, m.blobs, repo), nil
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

func (m *metadataStore) Blobs() iter.Seq[digest.Digest] {
	return func(yield func(digest.Digest) bool) {
		for digest := range m.blobs.digests {
			if !yield(digest) {
				return
			}
		}
	}
}

func newRepository(name string, blobs *blobs, repo *repository) store.Repository {
	return &repositoryStore{
		name:  name,
		blobs: blobs,
		repo:  repo,
	}
}

type repositoryStore struct {
	store.Repository

	name  string
	blobs *blobs
	repo  *repository
}

func (r *repositoryStore) GetBlob(digest digest.Digest) error {
	if _, ok := r.repo.blobs[digest]; ok {
		return nil
	}
	return fmt.Errorf("%w: %s", store.ErrRepositoryBlobNotFound, digest)
}

func (r *repositoryStore) PutBlob(digest digest.Digest) error {
	_, ok := r.blobs.digests[digest]
	if !ok {
		r.blobs.digests[digest] = owners{
			repositories: make(map[string]any),
		}
	}
	r.blobs.digests[digest].repositories[r.name] = nil
	r.repo.blobs[digest] = nil
	return nil
}

func (r *repositoryStore) DeleteBlob(digest digest.Digest) error {
	if _, ok := r.repo.blobs[digest]; ok {
		delete(r.blobs.digests[digest].repositories, r.name)
		if len(r.blobs.digests[digest].repositories) == 0 {
			delete(r.blobs.digests, digest)
		}

		delete(r.repo.blobs, digest)
		return nil
	}
	return fmt.Errorf("%w: %s", store.ErrRepositoryBlobNotFound, digest)
}

func (r *repositoryStore) GetManifest(digest digest.Digest) (store.Manifest, error) {
	if manifest, ok := r.repo.manifests[digest]; ok {
		return manifest.metadata, nil
	}
	return store.Manifest{}, store.ErrManifestNotFound
}

func (r *repositoryStore) PutManifest(digest digest.Digest, meta store.Manifest, refs store.References) error {
	r.repo.manifests[digest] = manifest{
		metadata:   meta,
		references: refs,
	}
	if refs.Config != "" {
		if owners, ok := r.blobs.digests[refs.Config]; ok {
			owners.repositories[r.name] = nil
		} else {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestConfigNotFound, refs.Config)
		}
	}
	r.repo.blobs[digest] = owners{}
	return nil
}

func (r *repositoryStore) DeleteManifest(id digest.Digest) ([]digest.Digest, error) {
	if manifest, ok := r.repo.manifests[id]; ok {
		deleted := make([]digest.Digest, 0)

		if manifest.references.Config != "" {
			delete(r.repo.blobs, manifest.references.Config)
			deleted = append(deleted, manifest.references.Config)
		}

		delete(r.repo.manifests, id)
		deleted = append(deleted, id)

		delete(r.repo.blobs, id)

		return deleted, nil
	}

	return nil, store.ErrManifestNotFound
}
