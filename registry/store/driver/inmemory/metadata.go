package inmemory2

import (
	"fmt"
	"iter"
	"maps"
	"slices"

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
		digests map[digest.Digest]owners
	}

	owners struct {
		repositories map[string]any
		manifests    map[digest.Digest]any
	}

	repository struct {
		blobs     map[digest.Digest]owners
		manifests map[digest.Digest]manifest
		tags      map[string]digest.Digest
	}

	manifest struct {
		owners
		metadata  store.Manifest
		refs      store.References
		referrers map[digest.Digest]any
		tags      map[string]any
	}
)

func (m *metadataStore) CreateRepository(name string) (store.Repository, error) {
	if _, ok := m.repositories[name]; ok {
		return nil, fmt.Errorf("%w: %s", store.ErrRepositoryExists, name)
	}

	repo := &repository{
		blobs:     make(map[digest.Digest]owners),
		manifests: make(map[digest.Digest]manifest),
		tags:      make(map[string]digest.Digest),
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
	_, ok := r.repo.blobs[digest]
	if !ok {
		return fmt.Errorf("%w: %s", store.ErrRepositoryBlobNotFound, digest)
	}
	return nil
}

func (r *repositoryStore) PutBlob(digest digest.Digest) error {
	_, ok := r.blobs.digests[digest]
	if !ok {
		r.blobs.digests[digest] = owners{
			repositories: make(map[string]any),
		}
	}
	r.blobs.digests[digest].repositories[r.name] = nil
	r.repo.blobs[digest] = newOwners()
	return nil
}

func (r *repositoryStore) DeleteBlob(digest digest.Digest) error {
	owners, ok := r.repo.blobs[digest]
	if !ok {
		return fmt.Errorf("%w: %s", store.ErrRepositoryBlobNotFound, digest)
	}
	if len(owners.manifests) != 0 {
		return fmt.Errorf("%w: %s", store.ErrBlobInUse, digest)
	}

	delete(r.blobs.digests[digest].repositories, r.name)
	if len(r.blobs.digests[digest].repositories) == 0 {
		delete(r.blobs.digests, digest)
	}

	delete(r.repo.blobs, digest)
	return nil
}

func (r *repositoryStore) GetManifest(digest digest.Digest) (store.Manifest, error) {
	manifest, ok := r.repo.manifests[digest]
	if !ok {
		return store.Manifest{}, store.ErrManifestNotFound
	}
	return manifest.metadata, nil
}

func (r *repositoryStore) PutManifest(digest digest.Digest, meta store.Manifest, refs store.References) error {
	r.repo.manifests[digest] = newManifest(meta, refs)

	if refs.Config != "" {
		owners, ok := r.repo.blobs[refs.Config]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestConfigNotFound, refs.Config)
		}
		owners.manifests[digest] = nil
	}

	for _, layerDigest := range refs.Layers {
		owners, ok := r.repo.blobs[layerDigest]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestLayerNotFound, layerDigest)
		}
		owners.manifests[digest] = nil
	}

	for _, manifestDigest := range refs.Manifests {
		manifest, ok := r.repo.manifests[manifestDigest]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestImageNotFound, manifestDigest)
		}
		manifest.manifests[digest] = nil
	}

	if refs.Subject != "" {
		manifest, ok := r.repo.manifests[refs.Subject]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestSubjectNotFound, refs.Subject)
		}
		manifest.referrers[digest] = nil
	}

	return r.PutBlob(digest)
}

func (r *repositoryStore) DeleteManifest(id digest.Digest) ([]digest.Digest, error) {
	manifest, ok := r.repo.manifests[id]
	if !ok {
		return nil, store.ErrManifestNotFound
	}

	if len(manifest.manifests) != 0 || len(manifest.tags) != 0 {
		return nil, fmt.Errorf("%w: %s", store.ErrManifestInUse, id)
	}

	deleted := make([]digest.Digest, 0)

	if manifest.refs.Config != "" {
		delete(r.repo.blobs[manifest.refs.Config].manifests, id)
		if len(r.repo.blobs[manifest.refs.Config].manifests) == 0 {
			delete(r.repo.blobs, manifest.refs.Config)
			deleted = append(deleted, manifest.refs.Config)
		}
	}

	for _, layerDigest := range manifest.refs.Layers {
		delete(r.repo.blobs[layerDigest].manifests, id)
		if len(r.repo.blobs[layerDigest].manifests) == 0 {
			delete(r.repo.blobs, layerDigest)
			deleted = append(deleted, layerDigest)
		}
	}

	for _, manifestDigest := range manifest.refs.Manifests {
		delete(r.repo.manifests[manifestDigest].manifests, id)
		if len(r.repo.manifests[manifestDigest].manifests) == 0 {
			delete(r.repo.manifests, manifestDigest)
			delete(r.repo.blobs, manifestDigest)
			deleted = append(deleted, manifestDigest)
		}
	}

	if manifest.refs.Subject != "" {
		delete(r.repo.manifests[manifest.refs.Subject].referrers, id)
	}

	for referrerDigest := range manifest.referrers {
		delete(r.repo.manifests, referrerDigest)
		deleted = append(deleted, referrerDigest)
	}

	delete(r.repo.manifests, id)
	deleted = append(deleted, id)

	if err := r.DeleteBlob(id); err != nil {
		return deleted, err
	}

	return deleted, nil
}

func (r *repositoryStore) ListReferrers(subject digest.Digest) ([]digest.Digest, error) {
	referrers := slices.Collect(
		maps.Keys(r.repo.manifests[subject].referrers),
	)
	return referrers, nil
}

func (r *repositoryStore) ListTags(count int, last string) ([]string, error) {
	return nil, nil
}

func (r *repositoryStore) GetTag(tag string) (digest.Digest, error) {
	digest, ok := r.repo.tags[tag]
	if !ok {
		return "", fmt.Errorf("%w: %s", store.ErrTagNotFound, tag)
	}
	return digest, nil
}

func (r *repositoryStore) PutTag(tag string, digest digest.Digest) error {
	manifest, ok := r.repo.manifests[digest]
	if !ok {
		return fmt.Errorf("%w: %s", store.ErrManifestNotFound, digest)
	}
	manifest.tags[tag] = nil
	r.repo.tags[tag] = digest
	return nil
}

func (r *repositoryStore) DeleteTag(tag string) ([]digest.Digest, error) {
	if _, ok := r.repo.tags[tag]; !ok {
		return nil, fmt.Errorf("%w: %s", store.ErrTagNotFound, tag)
	}

	deleted := make([]digest.Digest, 0)
	digest := r.repo.tags[tag]
	delete(r.repo.manifests, digest)
	deleted = append(deleted, digest)

	delete(r.repo.tags, tag)
	return deleted, nil
}

func newManifest(meta store.Manifest, refs store.References) manifest {
	return manifest{
		metadata:  meta,
		refs:      refs,
		owners:    newOwners(),
		referrers: make(map[digest.Digest]any),
		tags:      make(map[string]any),
	}
}

func newOwners() owners {
	return owners{
		repositories: make(map[string]any),
		manifests:    make(map[digest.Digest]any),
	}
}
