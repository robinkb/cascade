package inmemory

import (
	"slices"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry"
)

func NewMetadataStore() cascade.MetadataStore {
	return &MetadataStore{
		repositories: make(map[string]*Repository),
		blobs:        make(map[string]string),
	}
}

type (
	MetadataStore struct {
		repositories map[string]*Repository
		blobs        map[string]string
	}

	Repository struct {
		blobs          map[string]*Blob
		manifests      map[string]*Manifest
		tags           map[string]*Tag
		uploadSessions map[string]*cascade.UploadSession
	}

	Manifest struct {
		path      string
		mediaType string
	}

	Blob struct {
		path string
	}

	Tag struct {
		digest digest.Digest
	}
)

// TODO: Should probably be part of the MetadataStore interface?
func (s *MetadataStore) ensureRepositoryExists(name string) {
	if _, ok := s.repositories[name]; !ok {
		s.repositories[name] = &Repository{
			blobs:          make(map[string]*Blob),
			manifests:      make(map[string]*Manifest),
			tags:           make(map[string]*Tag),
			uploadSessions: make(map[string]*cascade.UploadSession),
		}
	}
}

func (s *MetadataStore) GetBlob(repository string, digest digest.Digest) (string, error) {
	if repo, ok := s.repositories[repository]; ok {
		if blob, ok := repo.blobs[digest.String()]; ok {
			return blob.path, nil
		}
	}
	return "", cascade.ErrBlobUnknown
}

func (s *MetadataStore) PutBlob(repository string, digest digest.Digest, path string) error {
	s.ensureRepositoryExists(repository)
	s.repositories[repository].blobs[digest.String()] = &Blob{
		path: path,
	}
	return nil
}

func (s *MetadataStore) DeleteBlob(repository string, digest digest.Digest) error {
	delete(s.repositories[repository].blobs, digest.String())
	return nil
}

func (s *MetadataStore) GetManifest(repository string, digest digest.Digest) (*cascade.ManifestMetadata, error) {
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

func (s *MetadataStore) PutManifest(repository string, digest digest.Digest, meta *cascade.ManifestMetadata) error {
	s.ensureRepositoryExists(repository)
	s.repositories[repository].manifests[digest.String()] = &Manifest{
		path:      meta.Path,
		mediaType: meta.MediaType,
	}
	return nil
}

func (s *MetadataStore) DeleteManifest(repository string, digest digest.Digest) error {
	if repo, ok := s.repositories[repository]; ok {
		delete(repo.manifests, digest.String())
	}
	return nil
}

func (s *MetadataStore) ListTags(repository string, count int, last string) ([]string, error) {
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

func (s *MetadataStore) GetTag(repository, tag string) (digest.Digest, error) {
	if repo, ok := s.repositories[repository]; ok {
		if tag, ok := repo.tags[tag]; ok {
			return tag.digest, nil
		}
	}
	return "", cascade.ErrManifestUnknown
}

func (s *MetadataStore) PutTag(repository, tag string, digest digest.Digest) error {
	s.ensureRepositoryExists(repository)

	s.repositories[repository].tags[tag] = &Tag{
		digest: digest,
	}
	return nil
}

func (s *MetadataStore) DeleteTag(repository, tag string) error {
	delete(s.repositories[repository].tags, tag)
	return nil
}

func (s *MetadataStore) GetUpload(repository, id string) (*cascade.UploadSession, error) {
	if repo, ok := s.repositories[repository]; ok {
		if session, ok := repo.uploadSessions[id]; ok {
			return session, nil
		}
	}
	return nil, cascade.ErrBlobUploadUnknown
}

func (s *MetadataStore) PutUpload(repository string, session *cascade.UploadSession) error {
	s.ensureRepositoryExists(repository)
	s.repositories[repository].uploadSessions[session.ID.String()] = session
	return nil
}

func (s *MetadataStore) DeleteUpload(repository string, sessionID string) error {
	delete(s.repositories[repository].uploadSessions, sessionID)
	return nil
}
