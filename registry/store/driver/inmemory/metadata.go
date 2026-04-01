package inmemory

import (
	"encoding/gob"
	"errors"
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
		SharedBlobs:  make(sharedBlobs),
		Repositories: make(repositories),
	}
}

type (
	// metadataStore implements the store.Metadata interface
	metadataStore struct {
		SharedBlobs  sharedBlobs
		Repositories repositories
	}
	// sharedBlobs represents the metadata of blobs in the shared blob store.
	sharedBlobs map[digest.Digest]sharedBlobOwners
	// sharedBlobOwners tracks which repositories own a shared blob.
	sharedBlobOwners map[string]any
	// repositories contains all repositories in the metadata store..
	repositories map[string]*repository
)

func (m *metadataStore) ListRepositories(count int, last string) ([]string, error) {
	names := slices.Collect(maps.Keys(m.Repositories))
	slices.Sort(names)

	if count == -1 || count > len(names) {
		count = len(names)
	}

	start := 0
	if last != "" {
		found := false
		for _, tag := range names {
			start++
			if tag == last {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: %s", store.ErrRepositoryNotFound, last)
		}
	}

	end := min(start+count, len(names))

	return names[start:end], nil
}

func (m *metadataStore) CreateRepository(name string) (store.Repository, error) {
	if _, ok := m.Repositories[name]; ok {
		return nil, fmt.Errorf("%w: %s", store.ErrRepositoryExists, name)
	}

	repo := &repository{
		Blobs:     make(repoBlobs),
		Manifests: make(manifests),
		Tags:      make(tags),
		Sessions:  make(sessions),
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
		delete(m.SharedBlobs[digest], name)
		if len(m.SharedBlobs[digest]) == 0 {
			delete(m.SharedBlobs, digest)
		}
	}

	delete(m.Repositories, name)
	return nil
}

func (m *metadataStore) Blobs() iter.Seq[digest.Digest] {
	return func(yield func(digest.Digest) bool) {
		for digest := range m.SharedBlobs {
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

func newRepository(name string, blobs sharedBlobs, repo *repository) store.Repository {
	return &repositoryStore{
		name:  name,
		blobs: blobs,
		repo:  repo,
	}
}

type (
	// repository implements the store.Repository interface.
	repositoryStore struct {
		name  string
		blobs sharedBlobs
		repo  *repository
	}
	// repository tracks the data of a repository in the metadata store.
	repository struct {
		Blobs     repoBlobs
		Manifests manifests
		Tags      tags
		Sessions  sessions
	}
	// repoBlobs tracks the blobs used by the repository.
	repoBlobs map[digest.Digest]repoBlobOwners
	// repoBlobOwners tracks which manifests reference a particular blob.
	repoBlobOwners map[digest.Digest]any
	// manifests tracks the manifests in the repository.
	manifests map[digest.Digest]manifest
	// manifest contains the metadata of a manifest.
	manifest struct {
		manifestOwners
		Metadata  store.Manifest
		Refs      store.References
		Referrers map[digest.Digest]any
	}
	// manifestOwners tracks which manifests and tags reference a particular manifest.
	manifestOwners struct {
		Manifests map[digest.Digest]any
		Tags      map[string]any
	}
	// tags tracks the tags in the repository.
	tags map[string]digest.Digest
	// sessions tracks the UploadSessions in the repository.
	sessions map[uuid.UUID]*store.UploadSession
)

func (r *repositoryStore) GetBlob(id digest.Digest) error {
	_, ok := r.repo.Blobs[id]
	if !ok {
		return fmt.Errorf("%w: %s", store.ErrBlobNotFound, id)
	}
	return nil
}

func (r *repositoryStore) PutBlob(id digest.Digest) error {
	_, ok := r.blobs[id]
	if !ok {
		r.blobs[id] = make(sharedBlobOwners)
	}
	r.blobs[id][r.name] = nil
	r.repo.Blobs[id] = make(repoBlobOwners)
	return nil
}

func (r *repositoryStore) DeleteBlob(id digest.Digest) error {
	owners, ok := r.repo.Blobs[id]
	if !ok {
		return fmt.Errorf("%w: %s", store.ErrBlobNotFound, id)
	}
	if len(owners) != 0 {
		return fmt.Errorf("%w: %s", store.ErrBlobInUse, id)
	}

	delete(r.blobs[id], r.name)
	if len(r.blobs[id]) == 0 {
		delete(r.blobs, id)
	}

	delete(r.repo.Blobs, id)
	return nil
}

func (r *repositoryStore) GetManifest(id digest.Digest) (store.Manifest, error) {
	manifest, ok := r.repo.Manifests[id]
	if !ok {
		return store.Manifest{}, store.ErrManifestNotFound
	}
	return manifest.Metadata, nil
}

func (r *repositoryStore) PutManifest(id digest.Digest, meta store.Manifest, refs store.References) error {
	r.repo.Manifests[id] = newManifest(meta, refs)

	if refs.Config != "" {
		owners, ok := r.repo.Blobs[refs.Config]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestConfigNotFound, refs.Config)
		}
		owners[id] = nil
	}

	for _, layerDigest := range refs.Layers {
		owners, ok := r.repo.Blobs[layerDigest]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestLayerNotFound, layerDigest)
		}
		owners[id] = nil
	}

	for _, manifestDigest := range refs.Manifests {
		manifest, ok := r.repo.Manifests[manifestDigest]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestImageNotFound, manifestDigest)
		}
		manifest.Manifests[id] = nil
	}

	if refs.Subject != "" {
		manifest, ok := r.repo.Manifests[refs.Subject]
		if !ok {
			return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestSubjectNotFound, refs.Subject)
		}
		manifest.Referrers[id] = nil
	}

	if err := r.PutBlob(id); err != nil {
		return err
	}

	r.repo.Blobs[id][id] = nil

	return nil
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
		delete(r.repo.Blobs[manifest.Refs.Config], id)
		if err := r.DeleteBlob(manifest.Refs.Config); err != nil {
			if !errors.Is(err, store.ErrBlobInUse) {
				return deleted, err
			}
		} else {
			deleted = append(deleted, manifest.Refs.Config)
		}
	}

	for _, layerDigest := range manifest.Refs.Layers {
		delete(r.repo.Blobs[layerDigest], id)
		if err := r.DeleteBlob(layerDigest); err != nil {
			if errors.Is(err, store.ErrBlobInUse) {
				continue
			}
			return deleted, err
		}
		deleted = append(deleted, layerDigest)
	}

	for _, manifestDigest := range manifest.Refs.Manifests {
		delete(r.repo.Manifests[manifestDigest].Manifests, id)
		digests, err := r.DeleteManifest(manifestDigest)
		if err != nil {
			if errors.Is(err, store.ErrManifestInUse) {
				continue
			}
			return deleted, err
		}
		deleted = append(deleted, digests...)
	}

	if manifest.Refs.Subject != "" {
		delete(r.repo.Manifests[manifest.Refs.Subject].Referrers, id)
	}

	for referrerDigest := range manifest.Referrers {
		digests, err := r.DeleteManifest(referrerDigest)
		if err != nil {
			if errors.Is(err, store.ErrManifestInUse) {
				continue
			}
			return deleted, err
		}
		deleted = append(deleted, digests...)
	}

	delete(r.repo.Manifests, id)
	deleted = append(deleted, id)

	delete(r.repo.Blobs[id], id)
	return deleted, r.DeleteBlob(id)
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
		found := false
		for _, tag := range tags {
			start++
			if tag == last {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: %s", store.ErrTagNotFound, last)
		}
	}

	end := min(start+count, len(tags))

	return tags[start:end], nil
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

	digest := r.repo.Tags[tag]
	delete(r.repo.Tags, tag)
	delete(r.repo.Manifests[digest].Tags, tag)

	return r.DeleteManifest(digest)
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
		Metadata: meta,
		Refs:     refs,
		manifestOwners: manifestOwners{
			Manifests: make(map[digest.Digest]any),
			Tags:      make(map[string]any),
		},
		Referrers: make(map[digest.Digest]any),
	}
}
