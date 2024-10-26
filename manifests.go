package cascade

import (
	"encoding/json"
	"errors"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry/paths"
)

func NewManifest(content []byte) (*Manifest, error) {
	var manifest Manifest
	err := json.Unmarshal(content, &manifest)
	if err != nil {
		err = ErrManifestInvalid
	}
	manifest.bytes = content

	return &manifest, err
}

type Manifest struct {
	v1.Manifest
	bytes []byte
}

func (m *Manifest) Bytes() []byte {
	return m.bytes
}

func (s *registryService) StatManifest(repository, id string) (*FileInfo, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrDigestInvalid
	}

	linkPath := paths.MetaStore.ManifestLink(repository, digest)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	dataPath := paths.BlobStore.BlobData(digest)
	info, err := s.store.Stat(dataPath)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrManifestUnknown
	}

	return info, err
}

func (s *registryService) GetManifest(repository, id string) (*Manifest, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	linkPath := paths.MetaStore.ManifestLink(repository, digest)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	dataPath := paths.BlobStore.BlobData(digest)
	content, err := s.store.Get(dataPath)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrManifestUnknown
	}

	return NewManifest(content)
}

func (s *registryService) PutManifest(repository, reference string, content []byte) error {
	digest, err := digest.Parse(reference)
	if err != nil {
		return ErrDigestInvalid
	}

	err = json.Unmarshal(content, &v1.Manifest{})
	if err != nil {
		return ErrManifestInvalid
	}

	dataPath := paths.BlobStore.BlobData(digest)
	linkPath := paths.MetaStore.ManifestLink(repository, digest)

	s.store.Set(dataPath, content)
	s.store.Set(linkPath, nil)

	return nil
}

func (s *registryService) DeleteManifest(repository, id string) error {
	digest, err := digest.Parse(id)
	if err != nil {
		return ErrManifestUnknown
	}

	linkPath := paths.MetaStore.ManifestLink(repository, digest)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return ErrManifestUnknown
	}

	dataPath := paths.BlobStore.BlobData(digest)
	s.store.Delete(dataPath)
	s.store.Delete(linkPath)

	return err
}
