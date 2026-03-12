package inmemory

import (
	"encoding/gob"
	"io"
	"slices"
	"sync"

	godigest "github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
)

func NewMetadataStore() store.Metadata {
	return &metadataStore{
		Repositories: make(map[string]*repository),
		Blobs:        make(map[godigest.Digest]map[string]struct{}),
	}
}

type (
	metadataStore struct {
		mu sync.RWMutex

		Repositories map[string]*repository
		Blobs        map[godigest.Digest]map[string]struct{}
	}

	repository struct {
		Blobs          map[string]*blob
		Manifests      map[string]*manifest
		Tags           map[string]*ttag
		UploadSessions map[string]*store.UploadSession
	}

	// TODO: This should be refactored to something like:
	// manifest struct {
	//   Metadata  ManifestMetadata
	//   Referrers map[godigest.Digest]any
	// }
	// This is how it's laid out in the BoltDB implementation.
	// It would allow for the Metadata to be extracted into a common stuct.
	manifest struct {
		Annotations  map[string]string
		ArtifactType string
		MediaType    string
		Referrers    map[godigest.Digest]any
		Size         int64
	}

	blob struct{}

	// Named 'ttag' instead of 'tag', because otherwise
	// this type would be shadowed by variables named 'tag'.
	ttag struct {
		Digest godigest.Digest
	}
)

func (s *metadataStore) CreateRepository(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.Repositories[name]; !ok {
		s.Repositories[name] = &repository{
			Blobs:          make(map[string]*blob),
			Manifests:      make(map[string]*manifest),
			Tags:           make(map[string]*ttag),
			UploadSessions: make(map[string]*store.UploadSession),
		}
	}
	return nil
}

func (s *metadataStore) GetRepository(name string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.Repositories[name]; !ok {
		return store.ErrRepositoryNotFound
	}
	return nil
}

func (s *metadataStore) DeleteRepository(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Repositories, name)
	return nil
}

func (s *metadataStore) ListBlobs() ([]godigest.Digest, error) {
	digests := make([]godigest.Digest, 0)
	for id := range s.Blobs {
		digests = append(digests, id)
	}
	return digests, nil
}

func (s *metadataStore) GetBlob(repository string, digest godigest.Digest) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if repo, ok := s.Repositories[repository]; ok {
		if _, ok := repo.Blobs[digest.String()]; ok {
			return "", nil
		}
	}
	return "", store.ErrNotFound
}

func (s *metadataStore) PutBlob(repository string, digest godigest.Digest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	repo, ok := s.Repositories[repository]
	if !ok {
		return store.ErrRepositoryNotFound
	}

	repo.Blobs[digest.String()] = &blob{}

	_, ok = s.Blobs[digest]
	if !ok {
		s.Blobs[digest] = make(map[string]struct{})
	}
	s.Blobs[digest][repository] = struct{}{}
	return nil
}

func (s *metadataStore) DeleteBlob(repository string, digest godigest.Digest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Repositories[repository].Blobs, digest.String())
	delete(s.Blobs[digest], repository)
	if len(s.Blobs[digest]) == 0 {
		delete(s.Blobs, digest)
	}
	return nil
}

func (s *metadataStore) GetManifest(repository string, digest godigest.Digest) (*store.ManifestMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	repo, ok := s.Repositories[repository]
	if !ok {
		return nil, store.ErrRepositoryNotFound
	}

	if manifest, ok := repo.Manifests[digest.String()]; ok {
		return &store.ManifestMetadata{
			Annotations:  manifest.Annotations,
			ArtifactType: manifest.ArtifactType,
			MediaType:    manifest.MediaType,
			Size:         manifest.Size,
		}, nil
	}
	return nil, store.ErrMetadataNotFound
}

func (s *metadataStore) PutManifest(repository string, digest godigest.Digest, meta *store.ManifestMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	manifests, ok := s.Repositories[repository].Manifests[digest.String()]
	if !ok {
		manifests = &manifest{
			Referrers: make(map[godigest.Digest]any),
		}
		s.Repositories[repository].Manifests[digest.String()] = manifests
	}

	manifests.Annotations = meta.Annotations
	manifests.ArtifactType = meta.ArtifactType
	manifests.MediaType = meta.MediaType
	manifests.Size = meta.Size

	if meta.Subject != "" {
		manifests, ok := s.Repositories[repository].Manifests[meta.Subject.String()]
		if !ok {
			manifests = &manifest{
				Referrers: make(map[godigest.Digest]any),
			}
			s.Repositories[repository].Manifests[meta.Subject.String()] = manifests
		}

		manifests.Referrers[digest] = nil
	}

	return nil
}

func (s *metadataStore) DeleteManifest(repository string, digest godigest.Digest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if repo, ok := s.Repositories[repository]; ok {
		delete(repo.Manifests, digest.String())
	}
	return nil
}

func (s *metadataStore) ListTags(repository string, count int, last string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tags := []string{}

	for key := range s.Repositories[repository].Tags {
		tags = append(tags, key)
	}

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

func (s *metadataStore) GetTag(repository, tag string) (godigest.Digest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if repo, ok := s.Repositories[repository]; ok {
		if tag, ok := repo.Tags[tag]; ok {
			return tag.Digest, nil
		}
	}
	return "", store.ErrNotFound
}

func (s *metadataStore) PutTag(repository, tag string, digest godigest.Digest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	repo, ok := s.Repositories[repository]
	if !ok {
		return store.ErrRepositoryNotFound
	}
	repo.Tags[tag] = &ttag{
		Digest: digest,
	}
	return nil
}

func (s *metadataStore) DeleteTag(repository, tag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Repositories[repository].Tags, tag)
	return nil
}

func (s *metadataStore) ListReferrers(repository string, digest godigest.Digest) ([]godigest.Digest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	repo, ok := s.Repositories[repository]
	if !ok {
		return nil, store.ErrNotFound
	}

	manifest, ok := repo.Manifests[digest.String()]
	if !ok {
		return []godigest.Digest{}, nil
	}

	referrers := make([]godigest.Digest, 0)

	for d := range manifest.Referrers {
		referrers = append(referrers, d)
	}

	return referrers, nil
}

func (s *metadataStore) GetUploadSession(repository, id string) (*store.UploadSession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	repo, ok := s.Repositories[repository]
	if !ok {
		return nil, store.ErrRepositoryNotFound
	}
	if session, ok := repo.UploadSessions[id]; ok {
		return session, nil
	}
	return nil, store.ErrNotFound
}

func (s *metadataStore) PutUploadSession(repository string, session *store.UploadSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	repo, ok := s.Repositories[repository]
	if !ok {
		return store.ErrRepositoryNotFound
	}
	repo.UploadSessions[session.ID.String()] = session
	return nil
}

func (s *metadataStore) DeleteUploadSession(repository string, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Repositories[repository].UploadSessions, sessionID)
	return nil
}

func (s *metadataStore) Snapshot(w io.Writer) error {
	return gob.NewEncoder(w).Encode(s)
}

func (s *metadataStore) Restore(r io.Reader) error {
	return gob.NewDecoder(r).Decode(s)
}
