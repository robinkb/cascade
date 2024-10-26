package cascade

import (
	"errors"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/paths"
)

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

func (s *registryService) GetManifest(repository, id string) ([]byte, error) {
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

	return content, err
}

func (s *registryService) PutManifest(repository, reference string, content []byte) error {
	digest, err := digest.Parse(reference)
	if err != nil {
		return ErrDigestInvalid
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
		return ErrBlobUnknown
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
