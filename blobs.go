package cascade

import (
	"errors"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/paths"
)

func (s *registryService) StatBlob(repository, id string) (*FileInfo, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	linkPath := paths.MetaStore.BlobLink(repository, digest)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	dataPath := paths.BlobStore.BlobData(digest)
	info, err := s.store.Stat(dataPath)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUnknown
	}

	return info, err
}

func (s *registryService) GetBlob(repository, id string) ([]byte, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	linkPath := paths.MetaStore.BlobLink(repository, digest)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	dataPath := paths.BlobStore.BlobData(digest)
	data, err := s.store.Get(dataPath)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUnknown
	}

	return data, err
}
