package inmemory

import (
	"io/fs"
	"slices"

	godigest "github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
)

func NewMetadataStore() store.Metadata {
	return &metadataStore{
		repositories: make(map[string]*repository),
	}
}

type (
	metadataStore struct {
		repositories map[string]*repository
	}

	repository struct {
		blobs          map[string]*blob
		manifests      map[string]*manifest
		tags           map[string]*ttag
		uploadSessions map[string]*store.UploadSession
	}

	manifest struct {
		annotations  map[string]string
		artifactType string
		mediaType    string
		referrers    map[godigest.Digest]any
		size         int64
	}

	blob struct{}

	// Named 'ttag' instead of 'tag', because otherwise
	// this type would be shadowed by variables named 'tag'.
	ttag struct {
		digest godigest.Digest
	}
)

func (s *metadataStore) CreateRepository(name string) error {
	if _, ok := s.repositories[name]; !ok {
		s.repositories[name] = &repository{
			blobs:          make(map[string]*blob),
			manifests:      make(map[string]*manifest),
			tags:           make(map[string]*ttag),
			uploadSessions: make(map[string]*store.UploadSession),
		}
	}
	return nil
}

func (s *metadataStore) GetBlob(repository string, digest godigest.Digest) (string, error) {
	if repo, ok := s.repositories[repository]; ok {
		if _, ok := repo.blobs[digest.String()]; ok {
			return "", nil
		}
	}
	return "", store.ErrNotFound
}

func (s *metadataStore) PutBlob(repository string, digest godigest.Digest) error {
	s.repositories[repository].blobs[digest.String()] = &blob{}
	return nil
}

func (s *metadataStore) DeleteBlob(repository string, digest godigest.Digest) error {
	delete(s.repositories[repository].blobs, digest.String())
	return nil
}

func (s *metadataStore) GetManifest(repository string, digest godigest.Digest) (*store.ManifestMetadata, error) {
	if repo, ok := s.repositories[repository]; ok {
		if manifest, ok := repo.manifests[digest.String()]; ok {
			return &store.ManifestMetadata{
				Annotations:  manifest.annotations,
				ArtifactType: manifest.artifactType,
				MediaType:    manifest.mediaType,
				Size:         manifest.size,
			}, nil
		}
	}
	return nil, fs.ErrNotExist
}

func (s *metadataStore) PutManifest(repository string, digest godigest.Digest, meta *store.ManifestMetadata) error {
	manifests, ok := s.repositories[repository].manifests[digest.String()]
	if !ok {
		manifests = &manifest{
			referrers: make(map[godigest.Digest]any),
		}
		s.repositories[repository].manifests[digest.String()] = manifests
	}

	manifests.annotations = meta.Annotations
	manifests.artifactType = meta.ArtifactType
	manifests.mediaType = meta.MediaType
	manifests.size = meta.Size

	if meta.Subject != "" {
		manifests, ok := s.repositories[repository].manifests[meta.Subject.String()]
		if !ok {
			manifests = &manifest{
				referrers: make(map[godigest.Digest]any),
			}
			s.repositories[repository].manifests[meta.Subject.String()] = manifests
		}

		manifests.referrers[digest] = nil
	}

	return nil
}

func (s *metadataStore) DeleteManifest(repository string, digest godigest.Digest) error {
	if repo, ok := s.repositories[repository]; ok {
		delete(repo.manifests, digest.String())
	}
	return nil
}

func (s *metadataStore) ListTags(repository string, count int, last string) ([]string, error) {
	tags := []string{}

	for key := range s.repositories[repository].tags {
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
	if repo, ok := s.repositories[repository]; ok {
		if tag, ok := repo.tags[tag]; ok {
			return tag.digest, nil
		}
	}
	return "", store.ErrNotFound
}

func (s *metadataStore) PutTag(repository, tag string, digest godigest.Digest) error {
	s.repositories[repository].tags[tag] = &ttag{
		digest: digest,
	}
	return nil
}

func (s *metadataStore) DeleteTag(repository, tag string) error {
	delete(s.repositories[repository].tags, tag)
	return nil
}

func (s *metadataStore) ListReferrers(repository string, digest godigest.Digest) ([]godigest.Digest, error) {
	repo, ok := s.repositories[repository]
	if !ok {
		return nil, store.ErrNotFound
	}

	manifest, ok := repo.manifests[digest.String()]
	if !ok {
		return []godigest.Digest{}, nil
	}

	referrers := make([]godigest.Digest, 0)

	for d := range manifest.referrers {
		referrers = append(referrers, d)
	}

	return referrers, nil
}

func (s *metadataStore) GetUploadSession(repository, id string) (*store.UploadSession, error) {
	if repo, ok := s.repositories[repository]; ok {
		if session, ok := repo.uploadSessions[id]; ok {
			return session, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *metadataStore) PutUploadSession(repository string, session *store.UploadSession) error {
	s.repositories[repository].uploadSessions[session.ID.String()] = session
	return nil
}

func (s *metadataStore) DeleteUploadSession(repository string, sessionID string) error {
	delete(s.repositories[repository].uploadSessions, sessionID)
	return nil
}
