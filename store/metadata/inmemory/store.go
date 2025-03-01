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

func (s *MetadataStore) GetBlob(repository string, digest digest.Digest) (string, error) {
	if repo, ok := s.repositories[repository]; ok {
		if blob, ok := repo.blobs[digest.String()]; ok {
			return blob.path, nil
		}
	}
	return "", cascade.ErrBlobUnknown
}

func (s *MetadataStore) PutBlob(repository string, digest digest.Digest, path string) error {
	s.repositories[repository].blobs[digest.String()] = &Blob{
		path: path,
	}
	return nil
}

func (s *MetadataStore) DeleteBlob(repository string, digest digest.Digest) error {
	delete(s.repositories[repository].blobs, digest.String())
	return nil
}

func (s *MetadataStore) GetManifest(repository string, digest digest.Digest) (string, error) {
	if repo, ok := s.repositories[repository]; ok {
		if manifest, ok := repo.manifests[digest.String()]; ok {
			return manifest.path, nil
		}
	}
	return "", cascade.ErrManifestUnknown
}

func (s *MetadataStore) PutManifest(repository string, digest digest.Digest, path string) error {
	s.repositories[repository].manifests[digest.String()] = &Manifest{
		path: path,
	}
	return nil
}

func (s *MetadataStore) DeleteManifest(repository string, digest digest.Digest) error {
	delete(s.repositories[repository].manifests, digest.String())
	return nil
}

func (s *MetadataStore) ListTags(repository string, count int, last string) ([]string, error) {
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
