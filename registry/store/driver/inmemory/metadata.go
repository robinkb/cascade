package inmemory

import (
	"encoding/gob"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
)

func NewMetadataStore() store.Metadata {
	return &metadataStore{
		SharedBlobs: &blobs{
			Digests: make(map[digest.Digest]owners),
		},
		Repositories: make(map[string]*repository),
	}
}

type (
	metadataStore struct {
		SharedBlobs  *blobs
		Repositories map[string]*repository
	}

	blobs struct {
		Digests map[digest.Digest]owners
	}

	owners struct {
		Repositories map[string]any
		Manifests    map[digest.Digest]any
	}

	repository struct {
		Blobs     map[digest.Digest]owners
		Manifests map[digest.Digest]manifest
		Tags      map[string]digest.Digest
		Sessions  map[uuid.UUID]*store.UploadSession
	}

	manifest struct {
		owners
		Metadata  store.Manifest
		Refs      store.References
		Referrers map[digest.Digest]any
		Tags      map[string]any
	}
)

func (m *metadataStore) CreateRepository(name string) (store.Repository, error) {
	if _, ok := m.Repositories[name]; ok {
		return nil, fmt.Errorf("%w: %s", store.ErrRepositoryExists, name)
	}

	repo := &repository{
		Blobs:     make(map[digest.Digest]owners),
		Manifests: make(map[digest.Digest]manifest),
		Tags:      make(map[string]digest.Digest),
		Sessions:  make(map[uuid.UUID]*store.UploadSession),
	}
	m.Repositories[name] = repo
	return newRepository(name, m.SharedBlobs, repo), nil
}

func (m *metadataStore) GetRepository(name string) (store.Repository, error) {
	if repo, ok := m.Repositories[name]; ok {
		return newRepository(name, m.SharedBlobs, repo), nil
	}
	return nil, fmt.Errorf("%w: %s", store.ErrRepositoryNotFound, name)
}

func (m *metadataStore) DeleteRepository(name string) error {
	repo, ok := m.Repositories[name]
	if !ok {
		return fmt.Errorf("%w: %s", store.ErrRepositoryNotFound, name)
	}

	for digest := range repo.Blobs {
		delete(m.SharedBlobs.Digests[digest].Repositories, name)
		if len(m.SharedBlobs.Digests[digest].Repositories) == 0 {
			delete(m.SharedBlobs.Digests, digest)
		}
	}

	delete(m.Repositories, name)
	return nil
}

func (m *metadataStore) Blobs() iter.Seq[digest.Digest] {
	return func(yield func(digest.Digest) bool) {
		for digest := range m.SharedBlobs.Digests {
			if !yield(digest) {
				return
			}
		}
	}
}

func (m *metadataStore) Snapshot(w io.Writer) error {
	return gob.NewEncoder(w).Encode(m)
}

func (m *metadataStore) Restore(r io.Reader) error {
	return gob.NewDecoder(r).Decode(m)
}

func newRepository(name string, blobs *blobs, repo *repository) store.Repository {
	return &repositoryStore{
		name:  name,
		blobs: blobs,
		repo:  repo,
	}
}

type repositoryStore struct {
	name  string
	blobs *blobs
	repo  *repository
}

func (r *repositoryStore) GetBlob(digest digest.Digest) error {
	_, ok := r.repo.Blobs[digest]
	if !ok {
		return fmt.Errorf("%w: %s", store.ErrRepositoryBlobNotFound, digest)
	}
	return nil
}

func (r *repositoryStore) PutBlob(digest digest.Digest) error {
	_, ok := r.blobs.Digests[digest]
	if !ok {
		r.blobs.Digests[digest] = owners{
			Repositories: make(map[string]any),
		}
	}
	r.blobs.Digests[digest].Repositories[r.name] = nil
	r.repo.Blobs[digest] = newOwners()
	return nil
}

func (r *repositoryStore) DeleteBlob(digest digest.Digest) error {
	owners, ok := r.repo.Blobs[digest]
	if !ok {
		return fmt.Errorf("%w: %s", store.ErrRepositoryBlobNotFound, digest)
	}
	if len(owners.Manifests) != 0 {
		return fmt.Errorf("%w: %s", store.ErrBlobInUse, digest)
	}

	delete(r.blobs.Digests[digest].Repositories, r.name)
	if len(r.blobs.Digests[digest].Repositories) == 0 {
		delete(r.blobs.Digests, digest)
	}

	delete(r.repo.Blobs, digest)
	return nil
}

func (r *repositoryStore) GetManifest(digest digest.Digest) (store.Manifest, error) {
	manifest, ok := r.repo.Manifests[digest]
	if !ok {
		return store.Manifest{}, store.ErrManifestNotFound
	}
	return manifest.Metadata, nil
}

func (r *repositoryStore) PutManifest(digest digest.Digest, meta store.Manifest, refs store.References) error {
	r.repo.Manifests[digest] = newManifest(meta, refs)

	if refs.Config != "" {
		owners, ok := r.repo.Blobs[refs.Config]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestConfigNotFound, refs.Config)
		}
		owners.Manifests[digest] = nil
	}

	for _, layerDigest := range refs.Layers {
		owners, ok := r.repo.Blobs[layerDigest]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestLayerNotFound, layerDigest)
		}
		owners.Manifests[digest] = nil
	}

	for _, manifestDigest := range refs.Manifests {
		manifest, ok := r.repo.Manifests[manifestDigest]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestImageNotFound, manifestDigest)
		}
		manifest.Manifests[digest] = nil
	}

	if refs.Subject != "" {
		manifest, ok := r.repo.Manifests[refs.Subject]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestSubjectNotFound, refs.Subject)
		}
		manifest.Referrers[digest] = nil
	}

	return r.PutBlob(digest)
}

func (r *repositoryStore) DeleteManifest(id digest.Digest) ([]digest.Digest, error) {
	manifest, ok := r.repo.Manifests[id]
	if !ok {
		return nil, store.ErrManifestNotFound
	}

	if len(manifest.Manifests) != 0 || len(manifest.Tags) != 0 {
		return nil, fmt.Errorf("%w: %s", store.ErrManifestInUse, id)
	}

	deleted := make([]digest.Digest, 0)

	if manifest.Refs.Config != "" {
		delete(r.repo.Blobs[manifest.Refs.Config].Manifests, id)
		if len(r.repo.Blobs[manifest.Refs.Config].Manifests) == 0 {
			delete(r.repo.Blobs, manifest.Refs.Config)
			deleted = append(deleted, manifest.Refs.Config)
		}
	}

	for _, layerDigest := range manifest.Refs.Layers {
		delete(r.repo.Blobs[layerDigest].Manifests, id)
		if len(r.repo.Blobs[layerDigest].Manifests) == 0 {
			delete(r.repo.Blobs, layerDigest)
			deleted = append(deleted, layerDigest)
		}
	}

	for _, manifestDigest := range manifest.Refs.Manifests {
		delete(r.repo.Manifests[manifestDigest].Manifests, id)
		if len(r.repo.Manifests[manifestDigest].Manifests) == 0 {
			delete(r.repo.Manifests, manifestDigest)
			delete(r.repo.Blobs, manifestDigest)
			deleted = append(deleted, manifestDigest)
		}
	}

	if manifest.Refs.Subject != "" {
		delete(r.repo.Manifests[manifest.Refs.Subject].Referrers, id)
	}

	for referrerDigest := range manifest.Referrers {
		delete(r.repo.Manifests, referrerDigest)
		deleted = append(deleted, referrerDigest)
	}

	delete(r.repo.Manifests, id)
	deleted = append(deleted, id)

	if err := r.DeleteBlob(id); err != nil {
		return deleted, err
	}

	return deleted, nil
}

func (r *repositoryStore) ListReferrers(subject digest.Digest) ([]digest.Digest, error) {
	referrers := slices.Collect(
		maps.Keys(r.repo.Manifests[subject].Referrers),
	)
	return referrers, nil
}

func (r *repositoryStore) ListTags(count int, last string) ([]string, error) {
	tags := slices.Collect(maps.Keys(r.repo.Tags))
	slices.Sort(tags)

	if count == -1 || count > len(tags) {
		count = len(tags)
	}

	start := 0
	if last != "" {
		for _, tag := range tags {
			start++
			if tag == last {
				break
			}
		}
	}

	if start+count > len(tags) {
		count -= start
	}

	return tags[start : start+count], nil
}

func (r *repositoryStore) GetTag(tag string) (digest.Digest, error) {
	digest, ok := r.repo.Tags[tag]
	if !ok {
		return "", fmt.Errorf("%w: %s", store.ErrTagNotFound, tag)
	}
	return digest, nil
}

func (r *repositoryStore) PutTag(tag string, digest digest.Digest) error {
	manifest, ok := r.repo.Manifests[digest]
	if !ok {
		return fmt.Errorf("%w: %s", store.ErrManifestNotFound, digest)
	}
	manifest.Tags[tag] = nil
	r.repo.Tags[tag] = digest
	return nil
}

func (r *repositoryStore) DeleteTag(tag string) ([]digest.Digest, error) {
	if _, ok := r.repo.Tags[tag]; !ok {
		return nil, fmt.Errorf("%w: %s", store.ErrTagNotFound, tag)
	}

	deleted := make([]digest.Digest, 0)
	digest := r.repo.Tags[tag]
	delete(r.repo.Manifests, digest)
	deleted = append(deleted, digest)

	delete(r.repo.Tags, tag)
	return deleted, nil
}

func (r *repositoryStore) GetUploadSession(id uuid.UUID) (*store.UploadSession, error) {
	session, ok := r.repo.Sessions[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", store.ErrUploadNotFound, id)
	}
	return session, nil
}

func (r *repositoryStore) PutUploadSession(session *store.UploadSession) error {
	r.repo.Sessions[session.ID] = session
	return nil
}

func (r *repositoryStore) DeleteUploadSession(id uuid.UUID) error {
	if _, ok := r.repo.Sessions[id]; !ok {
		return fmt.Errorf("%w: %s", store.ErrUploadNotFound, id)
	}
	delete(r.repo.Sessions, id)
	return nil
}

func newManifest(meta store.Manifest, refs store.References) manifest {
	return manifest{
		Metadata:  meta,
		Refs:      refs,
		owners:    newOwners(),
		Referrers: make(map[digest.Digest]any),
		Tags:      make(map[string]any),
	}
}

func newOwners() owners {
	return owners{
		Repositories: make(map[string]any),
		Manifests:    make(map[digest.Digest]any),
	}
}
