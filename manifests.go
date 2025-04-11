package cascade

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
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
		return nil, ErrManifestUnknown
	}

	path, err := s.metadata.GetManifest(repository, digest)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	info, err := s.blobs.Stat(path)
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

	path, err := s.metadata.GetManifest(repository, digest)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	content, err := s.blobs.Get(path)
	if err != nil {
		return nil, err
	}

	return NewManifest(content)
}

func (s *registryService) PutManifest(repository, reference string, content []byte) error {
	digest, err := digest.Parse(reference)
	if err != nil {
		return ErrDigestInvalid
	}

	var manifest v1.Manifest
	err = json.Unmarshal(content, &manifest)
	if err != nil {
		return ErrManifestInvalid
	}

	path := fmt.Sprintf("blobs/%s/%s/%s", digest.Algorithm(), digest.Encoded()[0:2], digest.Encoded())

	err = s.blobs.Put(path, content)
	if err != nil {
		return err
	}

	return s.metadata.PutManifest(repository, digest, &ManifestMetadata{
		Path:      path,
		MediaType: manifest.MediaType,
	})
}

func (s *registryService) DeleteManifest(repository, id string) error {
	digest, err := digest.Parse(id)
	if err != nil {
		return ErrManifestUnknown
	}

	path, err := s.metadata.GetManifest(repository, digest)
	if err != nil {
		return ErrManifestUnknown
	}

	err = s.blobs.Delete(path)
	if err != nil {
		return err
	}

	return s.metadata.DeleteManifest(repository, digest)
}
