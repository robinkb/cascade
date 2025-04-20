package inmemory

import (
	"slices"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry"
)

func NewMetadataStore() cascade.MetadataStore {
	return &metadataStore{
		repositories: make(map[string]*repository),
		blobs:        make(map[string]string),
	}
}

type (
	metadataStore struct {
		repositories map[string]*repository
		blobs        map[string]string
	}

	repository struct {
		blobs          map[string]*blob
		manifests      map[string]*manifest
		tags           map[string]*ttag
		uploadSessions map[string]*cascade.UploadSession
	}

	manifest struct {
		path      string
		mediaType string
	}

	blob struct {
		path string
	}

	// Named 'ttag' instead of 'tag', because otherwise
	// this type would be shadowed by variables named 'tag'.
	ttag struct {
		digest digest.Digest
	}
)

// TODO: Should probably be part of the MetadataStore interface?
func (s *metadataStore) ensureRepositoryExists(name string) {
	if _, ok := s.repositories[name]; !ok {
		s.repositories[name] = &repository{
			blobs:          make(map[string]*blob),
			manifests:      make(map[string]*manifest),
			tags:           make(map[string]*ttag),
			uploadSessions: make(map[string]*cascade.UploadSession),
		}
	}
}

func (s *metadataStore) GetBlob(repository string, digest digest.Digest) (string, error) {
	if repo, ok := s.repositories[repository]; ok {
		if blob, ok := repo.blobs[digest.String()]; ok {
			return blob.path, nil
		}
	}
	return "", cascade.ErrBlobUnknown
}

func (s *metadataStore) PutBlob(repository string, digest digest.Digest, path string) error {
	s.ensureRepositoryExists(repository)
	s.repositories[repository].blobs[digest.String()] = &blob{
		path: path,
	}
	return nil
}

func (s *metadataStore) DeleteBlob(repository string, digest digest.Digest) error {
	delete(s.repositories[repository].blobs, digest.String())
	return nil
}

func (s *metadataStore) GetManifest(repository string, digest digest.Digest) (*cascade.ManifestMetadata, error) {
	if repo, ok := s.repositories[repository]; ok {
		if manifest, ok := repo.manifests[digest.String()]; ok {
			return &cascade.ManifestMetadata{
				Path:      manifest.path,
				MediaType: manifest.mediaType,
			}, nil
		}
	}
	return nil, cascade.ErrManifestUnknown
}

func (s *metadataStore) PutManifest(repository string, digest digest.Digest, meta *cascade.ManifestMetadata) error {
	s.ensureRepositoryExists(repository)
	s.repositories[repository].manifests[digest.String()] = &manifest{
		path:      meta.Path,
		mediaType: meta.MediaType,
	}
	return nil
}

func (s *metadataStore) DeleteManifest(repository string, digest digest.Digest) error {
	if repo, ok := s.repositories[repository]; ok {
		delete(repo.manifests, digest.String())
	}
	return nil
}

func (s *metadataStore) ListTags(repository string, count int, last string) ([]string, error) {
	tags := []string{}

	s.ensureRepositoryExists(repository)
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

func (s *metadataStore) GetTag(repository, tag string) (digest.Digest, error) {
	if repo, ok := s.repositories[repository]; ok {
		if tag, ok := repo.tags[tag]; ok {
			return tag.digest, nil
		}
	}
	return "", cascade.ErrManifestUnknown
}

func (s *metadataStore) PutTag(repository, tag string, digest digest.Digest) error {
	s.ensureRepositoryExists(repository)

	s.repositories[repository].tags[tag] = &ttag{
		digest: digest,
	}
	return nil
}

func (s *metadataStore) DeleteTag(repository, tag string) error {
	delete(s.repositories[repository].tags, tag)
	return nil
}

func (s *metadataStore) GetUploadSession(repository, id string) (*cascade.UploadSession, error) {
	if repo, ok := s.repositories[repository]; ok {
		if session, ok := repo.uploadSessions[id]; ok {
			return session, nil
		}
	}
	return nil, cascade.ErrBlobUploadUnknown
}

func (s *metadataStore) PutUploadSession(repository string, session *cascade.UploadSession) error {
	s.ensureRepositoryExists(repository)
	s.repositories[repository].uploadSessions[session.ID.String()] = session
	return nil
}

func (s *metadataStore) DeleteUploadSession(repository string, sessionID string) error {
	delete(s.repositories[repository].uploadSessions, sessionID)
	return nil
}
